import socket
import threading
import time

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None

        self._create_socket()

    def _create_socket(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def handle_client(self, socket, address):
        print(f"[+] Connected to {address}")

        data = socket.recv(1024)

        if data:
            print(f"[!] Received: {data.decode()}")
            socket.sendall(data)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()

        print(f"[*] Server listening on {self.host}:{self.port}")

        sock, addr = self.server_socket.accept()
        self.handle_client(sock, addr)

        sock.close()
        self.server_socket.close()

        print(f"[-] Disconnected from {addr}")


class Client:
    def __init__(self):
        self.client_socket = None
        self._create_socket()

    def _create_socket(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, host, port):
        self.client_socket.connect((host, port))

    def send(self, data):
        self.client_socket.sendall(data)

    def receive(self, nbytes=0):
        return self.client_socket.recv(nbytes or 1024)

    def disconnect(self):
        self.client_socket.close()


server = Server('localhost', 8080)
threading.Thread(target=server.start, daemon=True).start()

time.sleep(2)

client = Client()
client.connect('localhost', 8080)
client.send("Hello, world".encode())
client.receive()
client.disconnect()