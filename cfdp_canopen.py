"""CFDP file transfer over CANopen SDO: fault handler, check timer, user callbacks, and OD factory."""

from __future__ import annotations

import logging
import threading
import time
from datetime import timedelta
from canopen import ObjectDictionary
from canopen.objectdictionary import ODRecord, ODVariable
from canopen.objectdictionary.datatypes import DOMAIN, UNSIGNED8, UNSIGNED32

from cfdppy import (
    CfdpUserBase,
    HostFilestore,
)

from cfdppy.mib import (
    CheckTimerProvider,
    DefaultFaultHandlerBase,
    EntityType,
)
from cfdppy.user import (
    FileSegmentRecvdParams,
    MetadataRecvParams,
    TransactionFinishedParams,
    TransactionParams,
)
from spacepackets.cfdp import (
    ConditionCode,
    FaultHandlerCode,
    TransactionId,
)
from spacepackets.countdown import Countdown

log = logging.getLogger("cfdp-canopen")

CFDP_PDU_OD_INDEX = 0x2000


# CFDP fault handler, timer, user
class LogFaults(DefaultFaultHandlerBase):
    def __init__(self, name: str):
        super().__init__()
        self.name = name
        for cond in (
            ConditionCode.POSITIVE_ACK_LIMIT_REACHED,
            ConditionCode.INACTIVITY_DETECTED,
            ConditionCode.CHECK_LIMIT_REACHED,
        ):
            self.set_handler(cond, FaultHandlerCode.ABANDON_TRANSACTION)

    def _log(self, label, transaction_id, cond, progress):
        log.info("[%s] Transaction %s %s: %s. Progress %s", self.name, transaction_id, label, cond, progress)

    def notice_of_suspension_cb(self, tid, c, p):
        self._log("suspended", tid, c, p)

    def notice_of_cancellation_cb(self, tid, c, p):
        self._log("cancelled", tid, c, p)

    def abandoned_cb(self, tid, c, p):
        self._log("abandoned", tid, c, p)

    def ignore_cb(self, tid, c, p):
        self._log("ignored", tid, c, p)


class DefaultCheckTimer(CheckTimerProvider):
    def provide_check_timer(self, local_entity_id, remote_entity_id, entity_type) -> Countdown:
        if entity_type == EntityType.SENDING:
            return Countdown(timedelta(seconds=5.0))
        return Countdown(timedelta(seconds=10.0))


class SimpleCfdpUser(CfdpUserBase):
    """Logs CFDP indications and signals transfer completion."""

    def __init__(self, name: str, vfs=None):
        super().__init__(vfs or HostFilestore())
        self.name = name
        self._finished_events: dict[TransactionId, threading.Event] = {}
        self._lock = threading.Lock()
        self.last_transaction_id: TransactionId | None = None
        self._pending_file_size: int | None = None
        self._transfer_start: dict[TransactionId, float] = {}
        self._transfer_size: dict[TransactionId, int] = {}
        self._transfer_result: dict[TransactionId, tuple[float, int]] = {}

    def finished_event(self, transaction_id: TransactionId) -> threading.Event:
        with self._lock:
            if transaction_id not in self._finished_events:
                self._finished_events[transaction_id] = threading.Event()
            return self._finished_events[transaction_id]

    def set_pending_file_size(self, file_size: int):
        """Set the file size for the next transfer (call before put_file)."""
        with self._lock:
            self._pending_file_size = file_size

    def get_transfer_speed(self, transaction_id: TransactionId) -> tuple[float, int] | None:
        """Return (elapsed_seconds, file_size) for a finished transfer, or None."""
        with self._lock:
            return self._transfer_result.get(transaction_id)

    def _complete_transaction(self, transaction_id: TransactionId):
        """Mark transaction finished and clean up stale events."""
        with self._lock:
            start = self._transfer_start.pop(transaction_id, None)
            size = self._transfer_size.pop(transaction_id, 0)
            if start is not None:
                self._transfer_result[transaction_id] = (time.monotonic() - start, size)
        self.finished_event(transaction_id).set()
        with self._lock:
            stale = {
                tid for tid, ev in self._finished_events.items()
                if ev.is_set() and tid != transaction_id
            }
            for tid in stale:
                self._finished_events.pop(tid, None)
                self._transfer_result.pop(tid, None)

    @staticmethod
    def _format_speed(size: int, elapsed: float) -> str:
        if elapsed <= 0:
            return f"{size} bytes"
        speed = size / elapsed
        if speed >= 1024 * 1024:
            return f"{size} bytes in {elapsed:.3f}s ({speed / (1024 * 1024):.2f} MiB/s)"
        if speed >= 1024:
            return f"{size} bytes in {elapsed:.3f}s ({speed / 1024:.2f} KiB/s)"
        return f"{size} bytes in {elapsed:.3f}s ({speed:.0f} B/s)"

    def _log_indication(self, label: str, **kwargs):
        parts = [f"{k}={v!r}" for k, v in kwargs.items()]
        log.info("[%s] %s: %s", self.name, label, ", ".join(parts) if parts else "")

    def transaction_indication(self, params: TransactionParams):
        self.last_transaction_id = params.transaction_id
        self.finished_event(params.transaction_id)  # pre-create
        with self._lock:
            self._transfer_start[params.transaction_id] = time.monotonic()
            size = self._pending_file_size
            if size is not None:
                self._transfer_size[params.transaction_id] = size
                self._pending_file_size = None
        self._log_indication("Transaction", transaction_id=params.transaction_id)

    def eof_sent_indication(self, transaction_id: TransactionId):
        self._log_indication("EOF Sent", transaction_id=transaction_id)

    def transaction_finished_indication(self, params: TransactionFinishedParams):
        tid = params.transaction_id
        self._complete_transaction(tid)
        self._log_indication(
            "Transaction Finished",
            transaction_id=tid,
            condition_code=params.finished_params.condition_code,
            delivery_code=params.finished_params.delivery_code,
            status=params.status_report,
        )
        result = self.get_transfer_speed(tid)
        if result is not None:
            elapsed, size = result
            log.info("[%s] Transfer speed: %s", self.name, self._format_speed(size, elapsed))

    def metadata_recv_indication(self, params: MetadataRecvParams):
        self._log_indication("Metadata Recv", transaction_id=params.transaction_id)

    def file_segment_recv_indication(self, params: FileSegmentRecvdParams):
        self._log_indication("File Segment Recv", transaction_id=params.transaction_id, offset=params.offset, length=params.length)

    def report_indication(self, transaction_id: TransactionId, status_report):
        self._log_indication("Report", transaction_id=transaction_id, status_report=status_report)

    def suspended_indication(self, transaction_id: TransactionId, cond_code: ConditionCode):
        self._log_indication("Suspended", transaction_id=transaction_id, cond_code=cond_code)

    def resumed_indication(self, transaction_id: TransactionId, progress: int):
        self._log_indication("Resumed", transaction_id=transaction_id, progress=progress)

    def fault_indication(self, transaction_id: TransactionId, cond_code: ConditionCode, progress: int):
        self._log_indication("Fault", transaction_id=transaction_id, cond_code=cond_code, progress=progress)
        self._complete_transaction(transaction_id)

    def abandoned_indication(self, transaction_id: TransactionId, cond_code: ConditionCode, progress: int):
        self._log_indication("Abandoned", transaction_id=transaction_id, cond_code=cond_code, progress=progress)
        self._complete_transaction(transaction_id)

    def eof_recv_indication(self, transaction_id: TransactionId):
        self._log_indication("EOF Recv", transaction_id=transaction_id)


# CANopen Object Dictionary for CFDP transport


def _make_var(name, index, subindex=0, dtype=UNSIGNED32, access="ro", default=0):
    v = ODVariable(name, index, subindex)
    v.data_type = dtype
    v.access_type = access
    v.default = default
    return v


def make_cfdp_od(node_id: int) -> ObjectDictionary:
    """Minimal OD with a DOMAIN entry at 0x2000 for PDU transport."""
    od = ObjectDictionary()

    # 0x1000 - Device Type: mandatory CANopen entry identifying the device profile
    od.add_object(_make_var("Device type", 0x1000))

    # 0x1018 - Identity Object: standard CANopen record for device identification
    # (vendor, product code, revision, serial — only vendor-ID used here)
    identity = ODRecord("Identity", 0x1018)
    identity.add_member(_make_var("Number of entries", 0x1018, 0, dtype=UNSIGNED8, default=1))
    identity.add_member(_make_var("Vendor-ID", 0x1018, 1))
    od.add_object(identity)

    # 0x2000 - CFDP PDU Inbox: manufacturer-specific DOMAIN entry used to
    # transport CFDP file-transfer PDUs over CANopen SDO
    od.add_object(_make_var("CFDP PDU Inbox", CFDP_PDU_OD_INDEX, dtype=DOMAIN, access="rw", default=b""))

    od.node_id = node_id
    return od
