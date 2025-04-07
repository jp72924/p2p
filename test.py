import socket
import threading
import time
import struct

def hello(socket=None, address=None):
    print("Hello!")

def handshake(socket=None, address=None):
    print("Handshake!")

class Router:
    def __init__(self):
        self.routes = {}

    def register_handler(self, route, handler):
        self.routes[route] = handler

    def handle_request(self, route, *args, **kwargs):
        if route in self.routes.keys():
            self.routes[route](*args, **kwargs)
        else:
            print("Unknown route")

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None
        self.router = None
        self.running = False
        self._create_socket()
        self._init_router()

    def _init_router(self):
        self.router = Router()
        self.router.register_handler("hello", hello)
        self.router.register_handler("handshake", handshake)

    def _create_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def _recv_all(self, sock, n):
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def handle_client(self, socket, address):
        print(f"[+] Connected to {address}")

        while True:
            # Read message length (4 bytes)
            raw_msglen = self._recv_all(socket, 4)
            if not raw_msglen:
                break
            
            msglen = struct.unpack('>I', raw_msglen)[0]
            # Read the message data
            data = self._recv_all(socket, msglen)
            if not data:
                break

            message = data.decode()
            print(f"[!] Received: {message}")
            
            # Send response with length prefix
            response = data
            socket.sendall(struct.pack('>I', len(response)) + response)

            self.router.handle_request(message, socket, address)

        print(f"[-] Disconnected from {address}")
        socket.close()

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.running = True

        print(f"[*] Server listening on {self.host}:{self.port}")

        while self.running:
            try:
                sock, addr = self.server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(sock, addr))
                client_thread.start()
            except OSError:
                break

    def stop(self):
        self.running = False
        self.server_socket.close()

class Client:
    def __init__(self):
        self.client_socket = None
        self._create_socket()

    def _create_socket(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def _recv_all(self, sock, n):
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def connect(self, host, port):
        self.client_socket.connect((host, port))

    def send(self, data):
        # Prefix each message with a 4-byte length (network byte order)
        self.client_socket.sendall(struct.pack('>I', len(data)) + data)

    def receive(self):
        # Read message length and unpack it into an integer
        raw_msglen = self._recv_all(self.client_socket, 4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # Read the message data
        data = self._recv_all(self.client_socket, msglen)
        return data if data else None

    def disconnect(self):
        self.client_socket.close()

# Create and start server
server = Server('localhost', 8080)
server_thread = threading.Thread(target=server.start)
server_thread.start()

time.sleep(1)  # Give server time to start

# Run client operations
client = Client()
client.connect('localhost', 8080)

client.send("hello".encode())
print(client.receive().decode())

client.send("handshake".encode())
print(client.receive().decode())

client.disconnect()

# Clean up server
server.stop()
server_thread.join()