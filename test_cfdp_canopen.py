"""Tests for cfdp_canopen.py."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from canopen.objectdictionary import ODVariable
from canopen.objectdictionary.datatypes import DOMAIN, UNSIGNED8, UNSIGNED32
from spacepackets.cfdp import ConditionCode, FaultHandlerCode, TransactionId
from spacepackets.cfdp.tlv import EntityIdTlv
from spacepackets.util import UnsignedByteField
from spacepackets.countdown import Countdown

from cfdppy.mib import EntityType

from cfdp_canopen import (
    CFDP_PDU_OD_INDEX,
    DefaultCheckTimer,
    LogFaults,
    SimpleCfdpUser,
    _make_var,
    make_cfdp_od,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_transaction_id(seq: int = 1, entity: int = 1) -> TransactionId:
    return TransactionId(
        source_entity_id=UnsignedByteField(entity, 2),
        transaction_seq_num=UnsignedByteField(seq, 2),
    )


# ---------------------------------------------------------------------------
# _make_var
# ---------------------------------------------------------------------------

class TestMakeVar:
    def test_defaults(self):
        v = _make_var("Test", 0x1000)
        assert isinstance(v, ODVariable)
        assert v.name == "Test"
        assert v.index == 0x1000
        assert v.subindex == 0
        assert v.data_type == UNSIGNED32
        assert v.access_type == "ro"
        assert v.default == 0

    def test_custom_params(self):
        v = _make_var("PDU", 0x2000, subindex=3, dtype=DOMAIN, access="rw", default=b"")
        assert v.index == 0x2000
        assert v.subindex == 3
        assert v.data_type == DOMAIN
        assert v.access_type == "rw"
        assert v.default == b""


# ---------------------------------------------------------------------------
# make_cfdp_od
# ---------------------------------------------------------------------------

class TestMakeCfdpOd:
    def test_has_required_objects(self):
        od = make_cfdp_od(5)
        assert 0x1000 in od
        assert 0x1018 in od
        assert CFDP_PDU_OD_INDEX in od

    def test_node_id(self):
        od = make_cfdp_od(42)
        assert od.node_id == 42

    def test_pdu_inbox_properties(self):
        od = make_cfdp_od(1)
        pdu = od[CFDP_PDU_OD_INDEX]
        assert pdu.data_type == DOMAIN
        assert pdu.access_type == "rw"

    def test_identity_record(self):
        od = make_cfdp_od(1)
        identity = od[0x1018]
        # subindex 0 = number of entries, subindex 1 = vendor-id
        assert 0 in identity
        assert 1 in identity
        assert identity[0].data_type == UNSIGNED8


# ---------------------------------------------------------------------------
# LogFaults
# ---------------------------------------------------------------------------

class TestLogFaults:
    def test_abandon_handlers_set(self):
        lf = LogFaults("test")
        for cond in (
            ConditionCode.POSITIVE_ACK_LIMIT_REACHED,
            ConditionCode.INACTIVITY_DETECTED,
            ConditionCode.CHECK_LIMIT_REACHED,
        ):
            assert lf.get_fault_handler(cond) == FaultHandlerCode.ABANDON_TRANSACTION

    def test_callbacks_run_without_error(self):
        lf = LogFaults("test")
        tid = _make_transaction_id()
        cond = ConditionCode.INACTIVITY_DETECTED
        lf.notice_of_suspension_cb(tid, cond, 0)
        lf.notice_of_cancellation_cb(tid, cond, 0)
        lf.abandoned_cb(tid, cond, 0)
        lf.ignore_cb(tid, cond, 0)


# ---------------------------------------------------------------------------
# DefaultCheckTimer
# ---------------------------------------------------------------------------

class TestDefaultCheckTimer:
    def test_sending_timer(self):
        t = DefaultCheckTimer()
        cd = t.provide_check_timer(1, 2, EntityType.SENDING)
        assert isinstance(cd, Countdown)
        assert cd.timeout == timedelta(seconds=5)

    def test_receiving_timer(self):
        t = DefaultCheckTimer()
        cd = t.provide_check_timer(1, 2, EntityType.RECEIVING)
        assert isinstance(cd, Countdown)
        assert cd.timeout == timedelta(seconds=10)


# ---------------------------------------------------------------------------
# SimpleCfdpUser._format_speed
# ---------------------------------------------------------------------------

class TestFormatSpeed:
    def test_zero_elapsed(self):
        result = SimpleCfdpUser._format_speed(100, 0)
        assert result == "100 bytes"

    def test_negative_elapsed(self):
        result = SimpleCfdpUser._format_speed(100, -1)
        assert result == "100 bytes"

    def test_bytes_per_second(self):
        result = SimpleCfdpUser._format_speed(500, 1.0)
        assert "B/s" in result
        assert "500 bytes" in result

    def test_kib_per_second(self):
        # 10240 bytes in 1s = 10 KiB/s
        result = SimpleCfdpUser._format_speed(10240, 1.0)
        assert "KiB/s" in result

    def test_mib_per_second(self):
        # 2 MiB in 1s
        size = 2 * 1024 * 1024
        result = SimpleCfdpUser._format_speed(size, 1.0)
        assert "MiB/s" in result


# ---------------------------------------------------------------------------
# SimpleCfdpUser – event & transaction tracking
# ---------------------------------------------------------------------------

class TestSimpleCfdpUser:
    @pytest.fixture()
    def user(self):
        return SimpleCfdpUser("test-user", vfs=MagicMock())

    def test_finished_event_returns_same_object(self, user):
        tid = _make_transaction_id()
        ev1 = user.finished_event(tid)
        ev2 = user.finished_event(tid)
        assert ev1 is ev2

    def test_finished_event_different_for_different_tid(self, user):
        t1 = _make_transaction_id(1)
        t2 = _make_transaction_id(2)
        assert user.finished_event(t1) is not user.finished_event(t2)

    def test_complete_transaction_sets_event(self, user):
        tid = _make_transaction_id()
        ev = user.finished_event(tid)
        assert not ev.is_set()
        user._complete_transaction(tid)
        assert ev.is_set()

    def test_complete_transaction_cleans_stale(self, user):
        t1 = _make_transaction_id(1)
        t2 = _make_transaction_id(2)
        user.finished_event(t1)
        user._complete_transaction(t1)
        # t1 is stale once t2 completes
        user.finished_event(t2)
        user._complete_transaction(t2)
        assert t1 not in user._finished_events

    def test_transfer_speed_tracking(self, user):
        tid = _make_transaction_id()
        user.set_pending_file_size(1024)
        # Simulate transaction_indication
        params = MagicMock()
        params.transaction_id = tid
        user.transaction_indication(params)
        assert user.last_transaction_id == tid
        # Complete
        user._complete_transaction(tid)
        result = user.get_transfer_speed(tid)
        assert result is not None
        elapsed, size = result
        assert size == 1024
        assert elapsed >= 0

    def test_fault_indication_completes_transaction(self, user):
        tid = _make_transaction_id()
        ev = user.finished_event(tid)
        user.fault_indication(tid, ConditionCode.INACTIVITY_DETECTED, 0)
        assert ev.is_set()

    def test_abandoned_indication_completes_transaction(self, user):
        tid = _make_transaction_id()
        ev = user.finished_event(tid)
        user.abandoned_indication(tid, ConditionCode.CHECK_LIMIT_REACHED, 0)
        assert ev.is_set()

    def test_indication_callbacks_run_without_error(self, user):
        tid = _make_transaction_id()
        user.eof_sent_indication(tid)
        user.eof_recv_indication(tid)
        user.report_indication(tid, "ok")
        user.suspended_indication(tid, ConditionCode.NO_ERROR)
        user.resumed_indication(tid, 0)

    def test_metadata_recv_indication(self, user):
        params = MagicMock()
        params.transaction_id = _make_transaction_id()
        user.metadata_recv_indication(params)

    def test_file_segment_recv_indication(self, user):
        params = MagicMock()
        params.transaction_id = _make_transaction_id()
        params.offset = 0
        params.length = 256
        user.file_segment_recv_indication(params)

    def test_transaction_finished_indication(self, user):
        tid = _make_transaction_id()
        user.set_pending_file_size(512)
        start_params = MagicMock()
        start_params.transaction_id = tid
        user.transaction_indication(start_params)

        fin_params = MagicMock()
        fin_params.transaction_id = tid
        fin_params.finished_params.condition_code = ConditionCode.NO_ERROR
        fin_params.finished_params.delivery_code = MagicMock()
        fin_params.status_report = None
        user.transaction_finished_indication(fin_params)
        assert user.finished_event(tid).is_set()

    def test_concurrent_finished_event(self, user):
        tid = _make_transaction_id()
        events = []

        def get_event():
            events.append(user.finished_event(tid))

        threads = [threading.Thread(target=get_event) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # All threads got the same event object
        assert all(e is events[0] for e in events)

    def test_get_transfer_speed_unknown_tid(self, user):
        tid = _make_transaction_id(99)
        assert user.get_transfer_speed(tid) is None

    def test_pending_file_size_consumed_once(self, user):
        """set_pending_file_size should be consumed by the next transaction_indication only."""
        user.set_pending_file_size(2048)
        t1 = _make_transaction_id(1)
        p1 = MagicMock()
        p1.transaction_id = t1
        user.transaction_indication(p1)

        t2 = _make_transaction_id(2)
        p2 = MagicMock()
        p2.transaction_id = t2
        user.transaction_indication(p2)

        # Check t1 speed before completing t2 (which would clean stale t1)
        user._complete_transaction(t1)
        r1 = user.get_transfer_speed(t1)
        assert r1 is not None and r1[1] == 2048

        user._complete_transaction(t2)
        r2 = user.get_transfer_speed(t2)
        assert r2 is not None and r2[1] == 0

    def test_complete_without_start_records_no_speed(self, user):
        """Completing a transaction that was never started should not crash."""
        tid = _make_transaction_id()
        user._complete_transaction(tid)
        assert user.get_transfer_speed(tid) is None

    def test_stale_cleanup_preserves_current(self, user):
        """Stale cleanup should not remove the transaction that just finished."""
        tids = [_make_transaction_id(i) for i in range(5)]
        for tid in tids:
            user.finished_event(tid)
            user._complete_transaction(tid)
        # Only the last one should remain
        last = tids[-1]
        assert last in user._finished_events
        for tid in tids[:-1]:
            assert tid not in user._finished_events

    def test_transaction_finished_logs_speed(self, user, caplog):
        """transaction_finished_indication should log transfer speed."""
        import logging
        tid = _make_transaction_id()
        user.set_pending_file_size(4096)
        start_params = MagicMock()
        start_params.transaction_id = tid
        user.transaction_indication(start_params)

        fin_params = MagicMock()
        fin_params.transaction_id = tid
        fin_params.finished_params.condition_code = ConditionCode.NO_ERROR
        fin_params.finished_params.delivery_code = MagicMock()
        fin_params.status_report = None
        with caplog.at_level(logging.INFO, logger="cfdp-canopen"):
            user.transaction_finished_indication(fin_params)
        assert any("Transfer speed" in msg for msg in caplog.messages)

    def test_fault_indication_records_speed(self, user):
        """If a transfer was started, fault should still record speed."""
        tid = _make_transaction_id()
        user.set_pending_file_size(512)
        params = MagicMock()
        params.transaction_id = tid
        user.transaction_indication(params)
        user.fault_indication(tid, ConditionCode.INACTIVITY_DETECTED, 100)
        result = user.get_transfer_speed(tid)
        assert result is not None
        assert result[1] == 512

    def test_abandoned_indication_records_speed(self, user):
        tid = _make_transaction_id()
        user.set_pending_file_size(256)
        params = MagicMock()
        params.transaction_id = tid
        user.transaction_indication(params)
        user.abandoned_indication(tid, ConditionCode.CHECK_LIMIT_REACHED, 50)
        result = user.get_transfer_speed(tid)
        assert result is not None
        assert result[1] == 256

    def test_last_transaction_id_updates(self, user):
        t1 = _make_transaction_id(1)
        t2 = _make_transaction_id(2)
        p1 = MagicMock()
        p1.transaction_id = t1
        p2 = MagicMock()
        p2.transaction_id = t2
        user.transaction_indication(p1)
        assert user.last_transaction_id == t1
        user.transaction_indication(p2)
        assert user.last_transaction_id == t2

    def test_default_vfs_is_host_filestore(self):
        """When vfs=None the default HostFilestore is used."""
        from cfdppy import HostFilestore
        u = SimpleCfdpUser("no-vfs")
        assert isinstance(u.vfs, HostFilestore)


# ---------------------------------------------------------------------------
# LogFaults – detailed callback logging
# ---------------------------------------------------------------------------

class TestLogFaultsLogging:
    def test_callbacks_log_messages(self, caplog):
        import logging
        lf = LogFaults("node-A")
        tid = _make_transaction_id()
        cond = ConditionCode.POSITIVE_ACK_LIMIT_REACHED
        with caplog.at_level(logging.INFO, logger="cfdp-canopen"):
            lf.notice_of_suspension_cb(tid, cond, 42)
        assert any("suspended" in msg for msg in caplog.messages)

    def test_cancellation_log(self, caplog):
        import logging
        lf = LogFaults("node-B")
        tid = _make_transaction_id()
        with caplog.at_level(logging.INFO, logger="cfdp-canopen"):
            lf.notice_of_cancellation_cb(tid, ConditionCode.INACTIVITY_DETECTED, 0)
        assert any("cancelled" in msg for msg in caplog.messages)

    def test_abandoned_log(self, caplog):
        import logging
        lf = LogFaults("node-C")
        tid = _make_transaction_id()
        with caplog.at_level(logging.INFO, logger="cfdp-canopen"):
            lf.abandoned_cb(tid, ConditionCode.CHECK_LIMIT_REACHED, 10)
        assert any("abandoned" in msg for msg in caplog.messages)

    def test_ignored_log(self, caplog):
        import logging
        lf = LogFaults("node-D")
        tid = _make_transaction_id()
        with caplog.at_level(logging.INFO, logger="cfdp-canopen"):
            lf.ignore_cb(tid, ConditionCode.NO_ERROR, 0)
        assert any("ignored" in msg for msg in caplog.messages)

    def test_name_stored(self):
        lf = LogFaults("my-node")
        assert lf.name == "my-node"

    def test_default_handlers_unchanged(self):
        """Conditions not explicitly set should retain the default handler."""
        lf = LogFaults("test")
        # NO_ERROR is not overridden in __init__
        default = lf.get_fault_handler(ConditionCode.NO_ERROR)
        assert default != FaultHandlerCode.ABANDON_TRANSACTION


# ---------------------------------------------------------------------------
# DefaultCheckTimer – edge cases
# ---------------------------------------------------------------------------

class TestDefaultCheckTimerEdgeCases:
    def test_different_entity_ids_same_result(self):
        """Timer values should not depend on entity IDs."""
        t = DefaultCheckTimer()
        cd1 = t.provide_check_timer(1, 2, EntityType.SENDING)
        cd2 = t.provide_check_timer(99, 100, EntityType.SENDING)
        assert cd1.timeout == cd2.timeout


# ---------------------------------------------------------------------------
# _format_speed – boundary values
# ---------------------------------------------------------------------------

class TestFormatSpeedBoundary:
    def test_exactly_1024_bytes_per_sec(self):
        # 1024 B/s should show KiB/s
        result = SimpleCfdpUser._format_speed(1024, 1.0)
        assert "KiB/s" in result

    def test_just_below_1024_bytes_per_sec(self):
        result = SimpleCfdpUser._format_speed(1023, 1.0)
        assert "B/s" in result

    def test_exactly_1_mib_per_sec(self):
        size = 1024 * 1024
        result = SimpleCfdpUser._format_speed(size, 1.0)
        assert "MiB/s" in result

    def test_just_below_1_mib_per_sec(self):
        size = 1024 * 1024 - 1
        result = SimpleCfdpUser._format_speed(size, 1.0)
        assert "KiB/s" in result

    def test_very_small_elapsed(self):
        # Extremely small but positive elapsed
        result = SimpleCfdpUser._format_speed(1, 0.000001)
        assert "MiB/s" in result or "KiB/s" in result or "B/s" in result

    def test_large_file(self):
        size = 1024 * 1024 * 100  # 100 MiB
        result = SimpleCfdpUser._format_speed(size, 10.0)
        assert "MiB/s" in result
        assert "10.00 MiB/s" in result


# ---------------------------------------------------------------------------
# make_cfdp_od – additional checks
# ---------------------------------------------------------------------------

class TestMakeCfdpOdExtra:
    def test_device_type_readonly(self):
        od = make_cfdp_od(1)
        assert od[0x1000].access_type == "ro"

    def test_identity_vendor_id_type(self):
        od = make_cfdp_od(1)
        assert od[0x1018][1].data_type == UNSIGNED32

    def test_pdu_inbox_default_empty_bytes(self):
        od = make_cfdp_od(1)
        assert od[CFDP_PDU_OD_INDEX].default == b""

    def test_different_node_ids(self):
        for nid in (0, 1, 127):
            od = make_cfdp_od(nid)
            assert od.node_id == nid
