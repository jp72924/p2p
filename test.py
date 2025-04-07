import socket
import threading
import time
import struct
from typing import Optional, Callable, Dict, Any


class FrameProtocol:
    HEADER_SIZE = 4  # 4-byte length prefix
    ENCODING = "utf-8"

    @staticmethod
    def encode_message(data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + data

    @staticmethod
    def decode_header(header: bytes) -> int:
        return struct.unpack(">I", header)[0]

    @classmethod
    def receive_message(cls, sock: socket.socket) -> Optional[bytes]:
        header = cls._recv_exact(sock, cls.HEADER_SIZE)
        if not header:
            return None
        msg_len = cls.decode_header(header)
        return cls._recv_exact(sock, msg_len)

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
        data = bytearray()
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)


class Router:
    def __init__(self):
        self.routes: Dict[str, Callable[..., Any]] = {}
        self.default_handler = self._default_handler

    def register(self, route: str, handler: Callable[..., Any]):
        self.routes[route] = handler
        return self  # Enable method chaining

    def set_default(self, handler: Callable[..., Any]):
        self.default_handler = handler
        return self

    def handle(self, route: str, *args, **kwargs) -> Any:
        handler = self.routes.get(route, self.default_handler)
        return handler(*args, **kwargs)

    @staticmethod
    def _default_handler(route: str, *args, **kwargs):
        print(f"No handler for route: {route}")
        return f"No handler for {route}"


class Server:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server_socket = None
        self.router = Router()
        self.running = False
        self._setup()

    def _setup(self):
        self._create_socket()
        self._init_routes()

    def _create_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def _init_routes(self):
        self.router.register("hello", self._handle_hello)
        self.router.register("handshake", self._handle_handshake)

    def _handle_hello(self, conn: socket.socket, address: tuple):
        print("Hello handler executed")
        conn.sendall(FrameProtocol.encode_message(b"Hello response"))

    def _handle_handshake(self, conn: socket.socket, address: tuple):
        print("Handshake handler executed")
        conn.sendall(FrameProtocol.encode_message(b"Handshake response"))

    def handle_client(self, conn: socket.socket, address: tuple):
        print(f"[+] Connection from {address}")
        try:
            while True:
                data = FrameProtocol.receive_message(conn)
                if not data:
                    break

                message = data.decode(FrameProtocol.ENCODING)
                print(f"[*] Received: {message}")
                # Pass only the message as route, conn and address as args
                response = self.router.handle(message, conn, address)
                if response:  # Only send if handler returned something
                    conn.sendall(FrameProtocol.encode_message(response.encode()))
        finally:
            print(f"[-] Disconnected {address}")
            conn.close()

    def start(self):
        print(f"[*] Listening on {self.host}:{self.port}")  # Moved before bind
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.running = True

        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except OSError:
                break

    def stop(self):
        self.running = False
        self.server_socket.close()


class Client:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, host: str, port: int):
        self.sock.connect((host, port))

    def send(self, message: str):
        self.sock.sendall(FrameProtocol.encode_message(message.encode()))

    def receive(self) -> Optional[str]:
        data = FrameProtocol.receive_message(self.sock)
        return data.decode() if data else None

    def close(self):
        self.sock.close()


def main():
    server = Server("localhost", 8080)
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(0.5)

    try:
        client = Client()
        client.connect("localhost", 8080)

        for message in ["hello", "handshake", "unknown"]:
            client.send(message)
            response = client.receive()
            print(f"Sent: {message} -> Received: {response}")
    finally:
        client.close()
        server.stop()
        server_thread.join(timeout=1)


if __name__ == "__main__":
    main()