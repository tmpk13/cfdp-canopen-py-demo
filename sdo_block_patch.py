"""Monkey-patch canopen.sdo.server.SdoServer to support SDO Block Download.

Call ``apply()`` before creating any LocalNode.  The patch adds a state
machine that handles the three phases of CiA 301 block download:

  1. INITIATE – client requests block transfer, server responds with blksize
  2. DATA     – client streams sequence frames, server ACKs per block
  3. END      – client sends CRC, server verifies and commits data
"""

from __future__ import annotations

import logging
import struct

from canopen.sdo.base import CrcXmodem
from canopen.sdo.constants import (
    ABORT_CRC_ERROR,
    ABORT_INVALID_COMMAND_SPECIFIER,
    BLOCK_SIZE_SPECIFIED,
    BLOCK_TRANSFER_RESPONSE,
    CRC_SUPPORTED,
    END_BLOCK_TRANSFER,
    INITIATE_BLOCK_TRANSFER,
    NO_MORE_BLOCKS,
    REQUEST_BLOCK_DOWNLOAD,
    RESPONSE_BLOCK_DOWNLOAD,
    SDO_STRUCT,
)
from canopen.sdo.server import SdoServer

log = logging.getLogger(__name__)

# Maximum sequences per block (CiA 301 allows 1-127).
_DEFAULT_BLKSIZE = 127


def _patched_init(orig_init):
    """Wrap __init__ to add block-download state variables."""

    def wrapper(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self._block_active = False
        self._block_expecting_end = False
        self._block_buffer = bytearray()
        self._block_seqno = 0
        self._block_blksize = _DEFAULT_BLKSIZE
        self._block_crc_supported = False

    return wrapper


def _patched_on_request(orig_on_request):
    """Wrap on_request to intercept frames while in block-download mode."""

    def wrapper(self, can_id, data, timestamp):
        if not self._block_active:
            return orig_on_request(self, can_id, data, timestamp)

        command = data[0]
        try:
            if self._block_expecting_end:
                # Expecting END_BLOCK_TRANSFER (CCS = REQUEST_BLOCK_DOWNLOAD)
                if command & 0xE0 == REQUEST_BLOCK_DOWNLOAD:
                    _handle_end(self, command, data)
                else:
                    self._block_active = False
                    self._block_expecting_end = False
                    self.abort(ABORT_INVALID_COMMAND_SPECIFIER)
            else:
                # Data phase – sequence frames
                _handle_sequence(self, data)
        except Exception:
            self._block_active = False
            self._block_expecting_end = False
            self.abort()
            log.exception("Block download failed")

    return wrapper


def _patched_block_download(self, data):
    """Handle INITIATE_BLOCK_TRANSFER request from client."""
    command, index, subindex = SDO_STRUCT.unpack_from(data)
    self._index = index
    self._subindex = subindex

    log.info("Block download initiate for 0x%04X:%02X", index, subindex)

    # Reset state
    self._block_buffer = bytearray()
    self._block_seqno = 0
    self._block_blksize = _DEFAULT_BLKSIZE
    self._block_crc_supported = bool(command & CRC_SUPPORTED)
    self._block_active = True
    self._block_expecting_end = False

    if command & BLOCK_SIZE_SPECIFIED:
        size = struct.unpack_from("<L", data, 4)[0]
        log.info("Client declared size: %d bytes", size)

    # Respond: RESPONSE_BLOCK_DOWNLOAD | INITIATE_BLOCK_TRANSFER | [CRC flag]
    res_command = RESPONSE_BLOCK_DOWNLOAD | INITIATE_BLOCK_TRANSFER
    if self._block_crc_supported:
        res_command |= CRC_SUPPORTED

    response = bytearray(8)
    SDO_STRUCT.pack_into(response, 0, res_command, index, subindex)
    response[4] = self._block_blksize
    self.send_response(response)


def _handle_sequence(server, data):
    """Process one sequence frame during block-download data phase."""
    command = data[0]
    seqno = command & 0x7F
    is_last = bool(command & NO_MORE_BLOCKS)

    server._block_seqno += 1

    # Accumulate 7 bytes of payload (padding trimmed later at END phase)
    server._block_buffer.extend(data[1:8])

    if is_last or seqno >= server._block_blksize:
        # Send block ACK
        response = bytearray(8)
        response[0] = RESPONSE_BLOCK_DOWNLOAD | BLOCK_TRANSFER_RESPONSE
        response[1] = seqno  # ackseq = last received seqno
        response[2] = server._block_blksize  # blksize for next block
        server.send_response(response)

        # Reset for next block
        server._block_seqno = 0

        if is_last:
            server._block_expecting_end = True


def _handle_end(server, command, data):
    """Process END_BLOCK_TRANSFER frame: verify CRC and commit data."""
    # Number of bytes in last frame that were NOT data
    n_unused = (command >> 2) & 0x7
    # Trim padding from buffer
    if n_unused > 0:
        del server._block_buffer[-n_unused:]

    # CRC check (computed on trimmed data — CRC-XMODEM is streaming,
    # so one pass over the full buffer equals the client's chunk-by-chunk calc)
    if server._block_crc_supported:
        crc = CrcXmodem()
        crc.process(server._block_buffer)
        client_crc = struct.unpack_from("<H", data, 1)[0]
        if crc.final() != client_crc:
            log.error(
                "Block download CRC mismatch: server=0x%04X client=0x%04X",
                crc.final(),
                client_crc,
            )
            server._block_active = False
            server._block_expecting_end = False
            server.abort(ABORT_CRC_ERROR)
            return

    # Commit data to object dictionary
    server._node.set_data(
        server._index,
        server._subindex,
        bytes(server._block_buffer),
        check_writable=True,
    )

    # Respond with END confirmation
    response = bytearray(8)
    response[0] = RESPONSE_BLOCK_DOWNLOAD | END_BLOCK_TRANSFER
    server.send_response(response)

    server._block_active = False
    server._block_expecting_end = False
    log.info(
        "Block download complete for 0x%04X:%02X (%d bytes)",
        server._index,
        server._subindex,
        len(server._block_buffer),
    )


def apply():
    """Monkey-patch SdoServer with block download support."""
    SdoServer.__init__ = _patched_init(SdoServer.__init__)
    SdoServer.on_request = _patched_on_request(SdoServer.on_request)
    SdoServer.block_download = _patched_block_download
    log.info("SDO block download patch applied")
