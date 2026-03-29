from __future__ import annotations

import argparse
import collections
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import can
import canopen

from cfdp_canopen import (
    CFDP_PDU_OD_INDEX,
    SimpleCfdpUser,
    LogFaults,
    DefaultCheckTimer,
    make_cfdp_od,
)
from cfdppy import (
    CfdpState,
    PacketDestination,
    PutRequest,
    get_packet_destination,
)
from cfdppy.handler.dest import DestHandler
from cfdppy.handler.source import SourceHandler
from cfdppy.mib import (
    IndicationConfig,
    LocalEntityConfig,
    RemoteEntityConfig,
    RemoteEntityConfigTable,
)
from spacepackets.cfdp import (
    ChecksumType,
    Direction,
    TransmissionMode,
)
from spacepackets.cfdp.pdu import PduFactory, PduHolder
from spacepackets.seqcount import SeqCountProvider
from spacepackets.util import ByteFieldU16

log = logging.getLogger("cfdp-node")

# Entity

class LiveEntity:

    def __init__(
        self,
        entity_id: int,
        total_entities: int,
        can_interface: str,
        can_channel: str,
    ):
        self.entity_id = entity_id
        self.name = f"Entity-{entity_id}"
        self.eid = ByteFieldU16(entity_id)

        peer_ids = [i for i in range(1, total_entities + 1) if i != entity_id]

        # CANopen setup
        self.bus = can.Bus(interface=can_interface, channel=can_channel)
        try:
            self.network = canopen.Network(bus=self.bus)

            od = make_cfdp_od(entity_id)
            self.local_node = canopen.LocalNode(entity_id, od)
            self.local_node.add_write_callback(self._on_od_write)
            self.network.add_node(self.local_node)

            self._remote_nodes: dict[int, canopen.RemoteNode] = {}
            for peer_eid in peer_ids:
                remote_od = make_cfdp_od(peer_eid)
                rnode = canopen.RemoteNode(peer_eid, remote_od)
                self.network.add_node(rnode)
                self._remote_nodes[peer_eid] = rnode

            self.network.connect()
        except Exception:
            self.bus.shutdown()
            raise

        # CFDP setup
        remote_cfgs = [
            RemoteEntityConfig(
                entity_id=ByteFieldU16(eid),
                max_file_segment_len=512,
                max_packet_len=512,
                closure_requested=True,
                crc_on_transmission=False,
                default_transmission_mode=TransmissionMode.ACKNOWLEDGED,
                crc_type=ChecksumType.CRC_32,
            )
            for eid in peer_ids
        ]
        remote_table = RemoteEntityConfigTable(remote_cfgs)
        self.user = SimpleCfdpUser(self.name)
        local_cfg = LocalEntityConfig(
            local_entity_id=self.eid,
            indication_cfg=IndicationConfig(),
            default_fault_handlers=LogFaults(self.name),
        )
        timer_prov = DefaultCheckTimer()

        self.source = SourceHandler(
            cfg=local_cfg,
            user=self.user,
            remote_cfg_table=remote_table,
            check_timer_provider=timer_prov,
            seq_num_provider=SeqCountProvider(16),
        )
        self.dest = DestHandler(
            cfg=local_cfg,
            user=self.user,
            remote_cfg_table=remote_table,
            check_timer_provider=timer_prov,
        )

        self._inbox: collections.deque[bytes] = collections.deque()
        self._lock = threading.Lock()


    # SDO server callback
    def _on_od_write(self, index: int, subindex: int, od, data: bytes):
        if index == CFDP_PDU_OD_INDEX:
            with self._lock:
                self._inbox.append(bytes(data))


    # PDU routing
    def _send_raw_pdu(self, dest_entity_id: int, raw: bytes):
        rnode = self._remote_nodes.get(dest_entity_id)
        if rnode is None:
            log.error("[%s] No route to entity %d", self.name, dest_entity_id)
            return
        try:
            rnode.sdo[CFDP_PDU_OD_INDEX].raw = raw
        except Exception:
            log.exception("[%s] SDO write to entity %d failed", self.name, dest_entity_id)


    def _collect_outgoing(self, handler) -> list[tuple[int, bytes]]:
        """Drain packets from handler. Must hold self._lock; send after releasing."""
        outgoing: list[tuple[int, bytes]] = []
        while handler.packets_ready:
            holder: Optional[PduHolder] = handler.get_next_packet()
            if holder is None:
                break
            pdu = holder.pdu
            if pdu is None:
                continue
            raw = holder.pack()
            if pdu.direction == Direction.TOWARDS_RECEIVER:
                target_eid = pdu.dest_entity_id.value
            else:
                target_eid = pdu.source_entity_id.value
            outgoing.append((target_eid, raw))
        return outgoing


    def _send_collected(self, outgoing: list[tuple[int, bytes]]):
        """Send collected PDUs over CAN (no lock held)."""
        for target_eid, raw in outgoing:
            self._send_raw_pdu(target_eid, raw)


    def _process_one_incoming(self, raw: bytes):
        pdu = PduFactory.from_raw(raw)
        if pdu is None:
            log.warning("[%s] Failed to parse PDU (%d bytes)", self.name, len(raw))
            return
        dest = get_packet_destination(pdu)
        with self._lock:
            if dest == PacketDestination.DEST_HANDLER:
                self.dest.state_machine(packet=pdu)
                outgoing = self._collect_outgoing(self.dest)
            else:
                self.source.state_machine(packet=pdu)
                outgoing = self._collect_outgoing(self.source)
        self._send_collected(outgoing)


    # Public API
    @property
    def peer_ids(self) -> list[int]:
        return sorted(self._remote_nodes.keys())

    def put_file(self, dest_entity_id: int, src_path: Path, dst_path: Path):
        req = PutRequest(
            destination_id=ByteFieldU16(dest_entity_id),
            source_file=src_path,
            dest_file=dst_path,
            trans_mode=TransmissionMode.ACKNOWLEDGED,
            closure_requested=True,
        )
        log.info("[%s] PUT %s -> entity %d:%s", self.name, src_path, dest_entity_id, dst_path)
        with self._lock:
            self.source.put_request(req)

    def step(self):
        with self._lock:
            outgoing: list[tuple[int, bytes]] = []
            if self.source.state != CfdpState.IDLE:
                self.source.state_machine()
                outgoing.extend(self._collect_outgoing(self.source))
            if self.dest.state != CfdpState.IDLE:
                self.dest.state_machine()
                outgoing.extend(self._collect_outgoing(self.dest))
        self._send_collected(outgoing)
        for _ in range(64):  # cap per tick
            with self._lock:
                if not self._inbox:
                    break
                raw = self._inbox.popleft()
            self._process_one_incoming(raw)

    def shutdown(self):
        self.network.disconnect()
        self.bus.shutdown()



_USE_COLOR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
_RED    = "\033[1;31m" if _USE_COLOR else ""
_RESET  = "\033[0m" if _USE_COLOR else ""


# Entry point
def main():
    parser = argparse.ArgumentParser(description="CFDP-over-CANopen node")
    parser.add_argument("entity_id", type=int, help="This node's entity ID (1, 2, ...)")
    parser.add_argument(
        "--total", type=int, default=2,
        help="Total number of entities in the system (default: 2)",
    )
    parser.add_argument(
        "--interface", default="socketcan",
        help="python-can interface (default: socketcan)",
    )
    parser.add_argument(
        "--channel", default="vcan0",
        help="CAN channel / device (default: vcan0)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s.%(msecs)03d [%(name)-18s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet noisy libraries
    for name in ("can.virtual", "canopen.sdo", "canopen.node", "canopen.network"):
        logging.getLogger(name).setLevel(logging.WARNING)

    if args.entity_id < 1 or args.entity_id > args.total:
        parser.error(f"entity_id must be between 1 and --total ({args.total})")

    print(f"Connecting to {args.interface}:{args.channel} ...")
    try:
        entity = LiveEntity(
            entity_id=args.entity_id,
            total_entities=args.total,
            can_interface=args.interface,
            can_channel=args.channel,
        )
    except Exception as exc:
        print(f"{_RED}Failed to connect: {exc}{_RESET}")
        if args.interface == "socketcan":
            print()
            print("  Bring up vcan0 first:")
            print("       sudo modprobe vcan && sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0")
            print()
            print("  Or use the in-process virtual bus (single terminal only):")
            print(f"       python cfdp_node.py {args.entity_id} --interface virtual --channel cfdp_vcan")
        sys.exit(1)

    from console import NodeConsole

    stop_event = threading.Event()

    def _stepper():
        while not stop_event.is_set():
            entity.step()
            time.sleep(0.002)

    stepper_thread = threading.Thread(target=_stepper, daemon=True, name="cfdp-step")
    stepper_thread.start()

    console = NodeConsole(entity)
    try:
        console.cmdloop()
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        stop_event.set()
        entity.shutdown()


if __name__ == "__main__":
    main()
