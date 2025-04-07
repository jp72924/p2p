import json
import socket
import struct
import threading
import queue
import time
import uuid
import logging
from abc import ABC, abstractmethod
from typing import Dict, Set, Tuple, Optional, Callable
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("PeerNode")

# ------------ Core Framework Components ------------
class ConnectionManager:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.peers: Dict[Tuple[str, int], socket.socket] = {}
        self.lock = threading.Lock()
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self._listen, daemon=False).start()  # Non-daemon thread
        logger.info(f"Started node on {self.host}:{self.port}")

    def _listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()

            while self.running:
                try:
                    conn, addr = server.accept()
                    logger.info(f"New connection from {addr}")
                    self._register_peer(conn, addr)
                except Exception as e:
                    logger.error(f"Connection error: {str(e)}")

    def _register_peer(self, sock: socket.socket, addr: Tuple[str, int]):
        with self.lock:
            self.peers[addr] = sock
        threading.Thread(target=self._handle_messages, args=(sock, addr), daemon=False).start()

    def _handle_messages(self, sock: socket.socket, addr: Tuple[str, int]):
        while self.running:
            try:
                data = self._receive_message(sock)
                if data:
                    logger.info(f"Received from {addr}: {data.decode()}")
            except Exception as e:
                logger.error(f"Message handling error: {str(e)}")
                break

    def _receive_message(self, sock: socket.socket) -> Optional[bytes]:
        try:
            header = sock.recv(4)
            if not header:
                return None
            length = struct.unpack(">I", header)[0]
            return sock.recv(length)
        except Exception:
            return None

    def broadcast(self, data: dict, exclude: Tuple[str, int] = None):
        """Send message to all connected peers except specified one"""
        serialized = json.dumps(data).encode()
        with self.lock:
            for addr, sock in self.peers.items():
                if addr != exclude:
                    try:
                        FrameProtocol.send(sock, serialized)
                    except Exception as e:
                        logger.error(f"Broadcast failed to {addr}: {str(e)}")
                        self._remove_peer(sock, addr)

    def connect(self, peer: Tuple[str, int], max_retries: int = 3, retry_delay: float = 1.0):
        for attempt in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(peer)
                logger.info(f"Connected to {peer}")
                self._register_peer(sock, peer)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Connection failed to {peer} after {max_retries} attempts: {str(e)}")
                else:
                    logger.warning(f"Connection attempt {attempt + 1} failed, retrying...")
                    time.sleep(retry_delay)

    def stop(self):
        self.running = False
        with self.lock:
            for sock in self.peers.values():
                sock.close()
            self.peers.clear()

class FrameProtocol:
    @staticmethod
    def send(sock: socket.socket, data: bytes):
        header = struct.pack(">I", len(data))
        sock.sendall(header + data)

    @staticmethod
    def receive(sock: socket.socket) -> Optional[bytes]:
        try:
            header = sock.recv(4)
            if not header:
                return None
            length = struct.unpack(">I", header)[0]
            return sock.recv(length)
        except Exception:
            return None

class MessageRouter:
    _handlers: Dict[str, Callable] = {}
    _middleware = []

    @classmethod
    def register_handler(cls, message_type: str, handler: Callable):
        cls._handlers[message_type] = handler

    @classmethod
    def add_middleware(cls, middleware_fn: Callable):
        cls._middleware.insert(0, middleware_fn)

    @classmethod
    def dispatch(cls, raw_data: bytes, source: Tuple[str, int]):
        try:
            data = json.loads(raw_data.decode())
            for middleware in cls._middleware:
                data = middleware(data, source)
                if not data:
                    return

            handler = cls._handlers.get(data.get('type'), cls._default_handler)
            handler(data, source)
        except Exception as e:
            logging.error(f"Message processing error: {str(e)}")

    @staticmethod
    def _default_handler(data: dict, source: Tuple[str, int]):
        logging.warning(f"No handler for message type: {data.get('type')}")

# ------------ Security Components ------------
@dataclass
class Session:
    peer_id: str
    socket: socket.socket
    challenge: Optional[bytes] = None
    authenticated: bool = False

class AuthProtocol:
    def __init__(self, network_secret: str):
        self.network_secret = network_secret.encode()
        self.sessions: Dict[Tuple[str, int], Session] = {}

    def _create_challenge(self) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=os.urandom(32),
            info=b'challenge',
            backend=default_backend()
        ).derive(self.network_secret)

    def handle_handshake(self, data: dict, source: Tuple[str, int]):
        if data['type'] == 'HELLO':
            session = Session(peer_id=data['node_id'], socket=None)
            session.challenge = self._create_challenge()
            self.sessions[source] = session
            FrameProtocol.send(source.socket, json.dumps({
                'type': 'CHALLENGE',
                'challenge': session.challenge.hex()
            }).encode())

        elif data['type'] == 'RESPONSE':
            session = self.sessions.get(source)
            if session and self._verify_response(session, data['response']):
                session.authenticated = True

    def _verify_response(self, session: Session, response: str) -> bool:
        expected = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=session.challenge,
            info=b'response',
            backend=default_backend()
        ).derive(self.network_secret)
        return expected == bytes.fromhex(response)

    def handle_challenge(self, data: dict, source: Tuple[str, int]):
        if data['type'] == 'CHALLENGE':
            challenge = bytes.fromhex(data['challenge'])
            response = self._create_response(challenge)
            
            FrameProtocol.send(
                self.sessions[source].socket,
                json.dumps({
                    'type': 'RESPONSE',
                    'response': response.hex()
                }).encode()
            )

# ------------ Application Layer ------------
class PeerDiscoveryProtocol:
    @staticmethod
    def handle_discovery(data: dict, source: Tuple[str, int]):
        if data['type'] == 'PEER_LIST_REQUEST':
            peers = ConnectionManager.get_active_peers()
            FrameProtocol.send(source.socket, json.dumps({
                'type': 'PEER_LIST',
                'peers': list(peers)
            }).encode())

class MessageForwardingProtocol:
    @staticmethod
    def handle_message(data: dict, source: Tuple[str, int], conn_mgr: ConnectionManager):
        if data['type'] == 'MESSAGE':
            conn_mgr.broadcast(data, exclude=source)

# ------------ Main Node Class ------------
class PeerNode:
    def __init__(self, host: str, port: int, config: dict):
        self.host = host
        self.port = port
        self.config = config

        # Initialize core components
        self.conn_mgr = ConnectionManager(host, port)
        self.auth = AuthProtocol(config['network_secret'])
        
        # Register protocols
        MessageRouter.register_handler('HELLO', self.auth.handle_handshake)
        MessageRouter.register_handler('PEER_LIST_REQUEST', PeerDiscoveryProtocol.handle_discovery)
        MessageRouter.register_handler('MESSAGE', MessageForwardingProtocol.handle_message)

        # Add middleware
        MessageRouter.add_middleware(self._auth_middleware)

        self.node_id = f"{host}:{port}"
        MessageRouter.register_handler('CHALLENGE', self.auth.handle_challenge)

    def _auth_middleware(self, data: dict, source: Tuple[str, int]) -> Optional[dict]:
        if not self.auth.sessions.get(source).authenticated:
            return None
        return data

    def _send_handshake(self):
        """Send initial HELLO message on connection"""
        handshake = {
            'type': 'HELLO',
            'node_id': self.node_id,
            'timestamp': time.time()
        }
        self.send_message(handshake)

    def connect(self, peer: Tuple[str, int]):
        """Override ConnectionManager connect to add handshake"""
        super().connect(peer)
        self._send_handshake()

    def start(self):
        self.conn_mgr.start()
        for peer in self.config['bootstrap_peers']:
            self.conn_mgr.connect(peer)

    def stop(self):
        self.conn_mgr.stop()

    def send_message(self, message: dict):
        MessageForwardingProtocol.handle_message({
            'type': 'MESSAGE',
            'payload': message,
            'id': str(uuid.uuid4())
        }, None, self.conn_mgr)  # Pass ConnectionManager instance


if __name__ == "__main__":
    config = {
        'network_secret': 'my-secure-network-key',
        'bootstrap_peers': [('127.0.0.1', 6000)]
    }

    # Create and start two peer nodes
    node1 = PeerNode('127.0.0.1', 6000, config)
    node2 = PeerNode('127.0.0.1', 6001, config)
    
    node1.start()
    time.sleep(1)  # Let node1 fully initialize
    node2.start()
    time.sleep(2)  # Ensure both nodes are ready
    
    # Send test message after connection
    time.sleep(2)  # Allow handshake to complete
    node1.send_message({'text': 'Hello Network!'})
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node1.stop()
        node2.stop()
        logger.info("Nodes stopped")