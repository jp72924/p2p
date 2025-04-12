import struct
import socket
from typing import Tuple, Optional


class MessageFramer:
    """
    Handles message framing with 4-byte big-endian length prefixes.
    Stateless and thread-safe (all methods are static).
    """
    HEADER_FORMAT = ">I"  # 4-byte unsigned integer (big-endian)
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @staticmethod
    def frame_message(data: bytes) -> bytes:
        """Add length prefix to raw bytes"""
        return struct.pack(MessageFramer.HEADER_FORMAT, len(data)) + data

    @staticmethod
    def deframe_data(buffer: bytes) -> Tuple[Optional[bytes], bytes]:
        """
        Attempt to extract a complete message from buffer.
        Returns (message, remaining_buffer)
        """
        if len(buffer) < MessageFramer.HEADER_SIZE:
            return None, buffer

        payload_len = struct.unpack(
            MessageFramer.HEADER_FORMAT,
            buffer[:MessageFramer.HEADER_SIZE]
        )[0]

        total_needed = MessageFramer.HEADER_SIZE + payload_len
        if len(buffer) < total_needed:
            return None, buffer

        message = buffer[MessageFramer.HEADER_SIZE:total_needed]
        remaining = buffer[total_needed:]
        return message, remaining

    @staticmethod
    def recv_message(sock: socket.socket) -> Optional[bytes]:
        """Receive complete framed message from socket"""
        try:
            header = MessageFramer._recv_exact(sock, MessageFramer.HEADER_SIZE)
            if not header:
                return None

            payload_len = struct.unpack(MessageFramer.HEADER_FORMAT, header)[0]
            payload = MessageFramer._recv_exact(sock, payload_len)
            return payload if payload else None
        except (ConnectionError, struct.error):
            return None

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
        """Internal: read exactly n bytes from socket"""
        data = bytearray()
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:  # EOF
                return None
            data.extend(chunk)
        return bytes(data)