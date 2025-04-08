import json
import socket
import struct
import threading
import time
import uuid
import logging
import os
from typing import Dict, Tuple, Optional, Callable
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
        self.auth = None  # Will be set by PeerNode

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

    def _remove_peer(self, sock: socket.socket, addr: Tuple[str, int]):
        with self.lock:
            if addr in self.peers:
                del self.peers[addr]
        if self.auth and addr in self.auth.sessions:  # Clean up Auth sessions
            del self.auth.sessions[addr]
        try:
            sock.close()
        except Exception:
            pass

    def _handle_messages(self, sock: socket.socket, addr: Tuple[str, int]):
        while self.running:
            try:
                data = FrameProtocol.receive(sock)  # Use FrameProtocol directly
                if data:
                    logger.info(f"Received from {addr}: {data.decode()}")
                    MessageRouter.dispatch(data, addr)  # ADD THIS LINE
            except Exception as e:
                logger.error(f"Message handling error: {str(e)}")
                self._remove_peer(sock, addr)  # Handle cleanup
                break

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

    def get_active_peers(self):
        with self.lock:
            return list(self.peers.keys())

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
                    retry_delay *= 2  # Exponential backoff
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
            logger.error(f"Message processing error: {str(e)}")  # Use configured logger

    @staticmethod
    def _default_handler(data: dict, source: Tuple[str, int]):
        logger.warning(f"No handler for message type: {data.get('type')}")

    @classmethod
    def _validation_middleware(cls, data: dict, source: tuple) -> Optional[dict]:
        required_fields = {
            'HELLO': ['node_id', 'timestamp'],
            'CHALLENGE': ['challenge', 'node_id'],
            'RESPONSE': ['response', 'node_id'],
            'MESSAGE': ['payload', 'id'],
            'PEER_LIST_REQUEST': [],
            'PEER_LIST': ['peers'],
            'AUTH_SUCCESS': ['message']
        }
        
        msg_type = data.get('type')
        if not msg_type:
            logger.warning(f"Message from {source} missing type field")
            return None
            
        if msg_type not in required_fields:
            logger.warning(f"Unknown message type '{msg_type}' from {source}")
            return None
            
        if not all(field in data for field in required_fields[msg_type]):
            logger.warning(f"Invalid {msg_type} message from {source} - missing required fields")
            return None
            
        return data

# ------------ Security Components ------------
@dataclass
class Session:
    socket: socket.socket
    challenge: Optional[bytes] = None
    authenticated: bool = False


class AuthProtocol:
    def __init__(self, network_secret: str, conn_mgr: ConnectionManager):  # Add ConnectionManager
        self.network_secret = network_secret.encode()
        self.sessions: Dict[Tuple[str, int], Session] = {}
        self.conn_mgr = conn_mgr  # Reference to ConnectionManager

    def _create_challenge(self) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=os.urandom(32),
            info=b'challenge',
            backend=default_backend()
        ).derive(self.network_secret)

    def _create_response(self, challenge: bytes) -> bytes:
        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=challenge,
            info=b'response',
            backend=default_backend()
        ).derive(self.network_secret)

    def handle_handshake(self, data: dict, source: Tuple[str, int]):
        logger.debug(f"Handling handshake message: {data} from {source}")
        if data['type'] == 'HELLO':
            with self.conn_mgr.lock:
                sock = self.conn_mgr.peers.get(source)
            if sock:
                session = Session(socket=sock)
                session.challenge = self._create_challenge()
                self.sessions[source] = session
                
                # Send challenge and include our own node ID
                FrameProtocol.send(sock, json.dumps({
                    'type': 'CHALLENGE',
                    'challenge': session.challenge.hex(),
                    'node_id': self.conn_mgr.node_id  # Add this line
                }).encode())

        elif data['type'] == 'RESPONSE':
            session = self.sessions.get(source)
            if session and self._verify_response(session, data['response']):
                session.authenticated = True
                # Send AUTH_SUCCESS to confirm
                FrameProtocol.send(session.socket, json.dumps({
                    'type': 'AUTH_SUCCESS',
                    'message': 'Authentication successful'
                }).encode())

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
        logger.debug(f"Handling challenge message: {data} from {source}")
        if data['type'] == 'CHALLENGE' and source in self.sessions:
            challenge = bytes.fromhex(data['challenge'])
            response = self._create_response(challenge)
            session = self.sessions[source]
            
            # Send response and expect AUTH_SUCCESS
            FrameProtocol.send(
                session.socket,
                json.dumps({
                    'type': 'RESPONSE',
                    'response': response.hex(),
                    'node_id': self.conn_mgr.node_id  # Add this line
                }).encode()
            )

    def handle_auth_success(self, data: dict, source: Tuple[str, int]):
        session = self.sessions.get(source)
        if session:
            session.authenticated = True
            logger.info(f"Authenticated with {source}")

# ------------ Application Layer ------------
class PeerDiscoveryProtocol:
    @staticmethod
    def handle_discovery(data: dict, source: Tuple[str, int], conn_mgr: ConnectionManager):
        if data['type'] == 'PEER_LIST_REQUEST':
            peers = conn_mgr.get_active_peers()
            with conn_mgr.lock:
                sock = conn_mgr.peers.get(source)
            if sock:
                FrameProtocol.send(sock, json.dumps({
                    'type': 'PEER_LIST',
                    'peers': list(peers)
                }).encode())

class MessageForwardingProtocol:
    @staticmethod
    def handle_message(data: dict, source: Tuple[str, int], conn_mgr: ConnectionManager):
        try:
            if data['type'] == 'MESSAGE':
                conn_mgr.broadcast(data, exclude=source)
        except KeyError:
            logger.error("Invalid message format received")

class DeduplicationMiddleware:
    def __init__(self):
        self.seen_messages = set()  # Tracks message IDs
        self.lock = threading.Lock()  # Thread-safe access

    def __call__(self, data: dict, source: Tuple[str, int]) -> Optional[dict]:
        """Middleware function to filter duplicates."""
        msg_type = data.get('type')
        # Skip ID check for handshake and auth messages
        if msg_type in ['HELLO', 'CHALLENGE', 'RESPONSE', 'AUTH_SUCCESS']:
            return data
            
        msg_id = data.get("id")
        if not msg_id:
            logger.warning(f"Message from {source} missing ID, dropping")
            return None

        with self.lock:
            if msg_id in self.seen_messages:
                logger.debug(f"Duplicate message {msg_id[:8]} from {source}, dropping")
                return None
            self.seen_messages.add(msg_id)

        return data

# ------------ Main Node Class ------------
class PeerNode:
    def __init__(self, host: str, port: int, config: dict):
        """
        Initialize a PeerNode with core components and message handlers.
        
        Args:
            host: The host address to bind to
            port: The port to listen on
            config: Configuration dictionary containing:
                - network_secret: Shared secret for authentication
                - bootstrap_peers: List of (host, port) tuples to connect to initially
        """
        self.host = host
        self.port = port
        self.config = config
        
        # Initialize core components
        self.conn_mgr = ConnectionManager(host, port)
        self.conn_mgr.node_id = f"{host}:{port}"  # Set node ID for connection manager
        self.conn_mgr.auth = self  # Reference back for auth cleanup
        
        # Initialize security components
        self.auth = AuthProtocol(config['network_secret'], self.conn_mgr)
        self.deduplicator = DeduplicationMiddleware()
        
        # Set node identifier
        self.node_id = f"{host}:{port}"
        
        # Register message handlers
        self._register_message_handlers()
        
        # Add middleware in correct order
        self._setup_middleware()
        
        # Track active connections
        self.active_connections = set()

    def _register_message_handlers(self):
        """Register all message handlers with the MessageRouter"""
        # Authentication protocol handlers
        MessageRouter.register_handler('HELLO', self.auth.handle_handshake)
        MessageRouter.register_handler('CHALLENGE', self.auth.handle_challenge)
        MessageRouter.register_handler('RESPONSE', self.auth.handle_handshake)
        MessageRouter.register_handler('AUTH_SUCCESS', self.auth.handle_auth_success)
        
        # Peer discovery handlers
        MessageRouter.register_handler(
            'PEER_LIST_REQUEST',
            lambda data, src: PeerDiscoveryProtocol.handle_discovery(data, src, self.conn_mgr)
        )
        
        # Message forwarding handler
        MessageRouter.register_handler(
            'MESSAGE',
            lambda data, src: MessageForwardingProtocol.handle_message(data, src, self.conn_mgr)
        )
        
        # Add any additional custom handlers here
        # MessageRouter.register_handler('CUSTOM_TYPE', self.handle_custom_type)

    def _setup_middleware(self):
        """Configure middleware pipeline in correct processing order"""
        # Clear any existing middleware
        MessageRouter._middleware.clear()
        
        # Add middleware in processing order (first added = last executed)
        
        # 1. Validation (should run first to catch malformed messages early)
        MessageRouter.add_middleware(MessageRouter._validation_middleware)
        
        # 2. Authentication (checks auth state after validation)
        MessageRouter.add_middleware(self._auth_middleware)
        
        # 3. Deduplication (should run after auth to avoid processing duplicates)
        MessageRouter.add_middleware(self.deduplicator)
        
        # Add any additional custom middleware here
        # MessageRouter.add_middleware(self._custom_middleware)

    def _auth_middleware(self, data: dict, source: Tuple[str, int]) -> Optional[dict]:
        """
        Middleware to enforce authentication for non-handshake messages.
        Allows internal messages and auth protocol messages to pass through.
        """
        # Skip auth check for internal messages and auth protocol messages
        if data.get('__internal__') or data.get('type') in ['HELLO', 'CHALLENGE', 'RESPONSE', 'AUTH_SUCCESS']:
            return data
            
        # Check authentication status
        session = self.auth.sessions.get(source)
        if not session or not session.authenticated:
            logger.warning(f"Unauthenticated message from {source}")
            return None
            
        return data

    def _send_handshake(self, peer: Tuple[str, int]):
        """Send handshake to specific peer"""
        with self.conn_mgr.lock:
            sock = self.conn_mgr.peers.get(peer)
        if sock:
            # Create session for the peer
            session = Session(socket=sock)
            self.auth.sessions[peer] = session
            handshake = {
                'type': 'HELLO',
                'node_id': self.node_id,
                'timestamp': time.time()
            }
            FrameProtocol.send(sock, json.dumps(handshake).encode())

    def connect(self, peer: Tuple[str, int]):
        """Override ConnectionManager connect to add handshake"""
        self.conn_mgr.connect(peer)
        self._send_handshake(peer)

    def start(self):
        self.conn_mgr.start()
        for peer in self.config['bootstrap_peers']:
            if peer != (self.host, self.port):  # Prevent self-connection
                self.connect(peer)

    def stop(self):
        self.conn_mgr.stop()

    def send_message(self, message: dict):
        MessageForwardingProtocol.handle_message({
            'type': 'MESSAGE',
            'payload': message,
            'id': str(uuid.uuid4()),
            '__internal__': True  # Skip auth middleware
        }, (self.host, self.port), self.conn_mgr)


if __name__ == "__main__":
    config1 = {
        'network_secret': 'my-secure-network-key',
        'bootstrap_peers': []
    }

    config2 = {
        'network_secret': 'my-secure-network-key',
        'bootstrap_peers': [('127.0.0.1', 8000)]
    }

    # Create and start two peer nodes
    node1 = PeerNode('127.0.0.1', 8000, config1)
    node2 = PeerNode('127.0.0.1', 8001, config2)
    
    node1.start()
    time.sleep(1)  # Let node1 fully initialize
    node2.start()
    time.sleep(2)  # Ensure both nodes are ready
    
    # Send test message after connection
    time.sleep(2)  # Allow handshake to complete
    
    try:
        while True:
            time.sleep(1)
            node1.send_message({'text': 'Hello Network!'})
    except KeyboardInterrupt:
        node1.stop()
        node2.stop()
        logger.info("Nodes stopped")