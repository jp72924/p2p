import socket
import threading
import logging
import json
import time
import uuid
import struct
from typing import Dict, Tuple, Set, Optional, Callable
import random

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

class FrameProtocol:
    """Generic message framing with 4-byte length prefix (big-endian)"""
    HEADER_SIZE = 4  # Bytes for message length
    ENCODING = "utf-8"

    @staticmethod
    def encode(data: str) -> bytes:
        """Convert string to framed bytes (length + payload)"""
        data_bytes = data.encode(FrameProtocol.ENCODING)
        return struct.pack(">I", len(data_bytes)) + data_bytes

    @staticmethod
    def decode(raw: bytes) -> str:
        """Extract string from framed bytes"""
        return raw.decode(FrameProtocol.ENCODING)

    @classmethod
    def read_message(cls, sock: socket.socket) -> Optional[str]:
        """Read full message from socket using framing"""
        try:
            # Read header
            header = cls._recv_exact(sock, cls.HEADER_SIZE)
            if not header:
                return None
            
            # Extract payload length
            payload_len = struct.unpack(">I", header)[0]
            
            # Read payload
            payload = cls._recv_exact(sock, payload_len)
            return cls.decode(payload) if payload else None
        except (ConnectionError, struct.error):
            return None

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
        """Read exactly `n` bytes from socket"""
        data = bytearray()
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

class MessageProcessor:
    """Handles different types of messages with registered handlers"""
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
        self.default_handler = self._handle_default_message

    def register_handler(self, message_type: str, handler: Callable):
        """Register a handler for a specific message type"""
        self.handlers[message_type] = handler

    def process_message(self, message: str, connection: Tuple[str, int], server: 'NetworkServer'):
        """Process incoming message based on its type"""
        try:
            msg_data = json.loads(message)
            if not isinstance(msg_data, dict):
                logging.warning("Received non-object message from %s", connection)
                raise ValueError("Message must be a JSON object")
            
            msg_type = msg_data.get("type", "default")
            logging.debug("Processing %s message from %s", msg_type, connection)
            handler = self.handlers.get(msg_type, self.default_handler)
            return handler(msg_data, connection, server)
        except json.JSONDecodeError as e:
            logging.warning("Invalid JSON from %s: %s", connection, message[:100])
            return self.default_handler({"content": message}, connection, server)
        except Exception as e:
            logging.error("Message processing failed from %s", connection, 
                         exc_info=True)
            return None

    def _handle_default_message(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """
        Default handler for unregistered message types.
        Converts unknown message types into proper broadcasts with deduplication.
        """
        content = message.get("content", "")
        if not content:
            return None

        # Convert to proper broadcast format with all required fields
        broadcast_msg = {
            "type": "broadcast",
            "message_id": str(uuid.uuid4()),  # Generate unique ID
            "sender": f"{connection[0]}:{connection[1]}",
            "content": content,
            "timestamp": time.time(),
        }

        # Add to seen messages before processing
        with server.dedup_lock:
            server.seen_message_ids.add(broadcast_msg['message_id'])

        # Process through the standard broadcast handler
        server._handle_broadcast(broadcast_msg, connection, server)
        return None

class NetworkServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        
        # Connection tracking
        self.incoming_connections: Dict[Tuple[str, int], socket.socket] = {}
        self.outgoing_connections: Dict[Tuple[str, int], socket.socket] = {}
        self.known_peers: Set[Tuple[str, int]] = set()
        
        # Message deduplication
        self.seen_message_ids: Set[str] = set()
        self.max_seen_messages = 1000  # Prevent memory leak
        self.dedup_lock = threading.Lock()

        # Message processing
        self.message_processor = MessageProcessor()
        self._register_handlers()
        
        # Thread management
        self.threads = []

    def _register_handlers(self):
        """Register protocol message handlers"""
        self.message_processor.register_handler("port_announce", self._handle_port_announce)
        self.message_processor.register_handler("ping", self._handle_ping)
        self.message_processor.register_handler("broadcast", self._handle_broadcast)

    def broadcast_message(self, message: dict, exclude_address: Optional[Tuple[str, int]] = None) -> int:
        """
        Broadcast a framed message to all connected peers with deduplication.
        Returns number of successful transmissions.
        
        Args:
            message: Dictionary containing the message payload
            exclude_address: Optional (host, port) tuple to exclude from broadcast
            
        Returns:
            int: Number of peers successfully sent the message
        """
        if not isinstance(message, dict):
            logging.error("Broadcast message must be a dictionary (got %s)", type(message))
            return 0

        # 1. Prepare the message with required fields
        message.setdefault('type', 'broadcast')
        if 'message_id' not in message:
            message['message_id'] = str(uuid.uuid4())
        if 'sender' not in message:
            message['sender'] = f"{self.host}:{self.port}"
        if 'timestamp' not in message:
            message['timestamp'] = time.time()

        # 2. Deduplication setup
        with self.dedup_lock:
            if message['message_id'] in self.seen_message_ids:
                logging.debug(f"Message {message['message_id']} already broadcasted")
                return 0
                
            self.seen_message_ids.add(message['message_id'])
            # Rotate seen messages if exceeds limit
            if len(self.seen_message_ids) > self.max_seen_messages:
                self.seen_message_ids = set(list(self.seen_message_ids)[-self.max_seen_messages:])

        # 3. Convert to framed message (once, for efficiency)
        try:
            message_str = json.dumps(message)
            framed_data = FrameProtocol.encode(message_str)
        except (TypeError, json.JSONEncodeError) as e:
            logging.error(f"Failed to serialize broadcast message: {e}")
            return 0

        # 4. Send to all active connections
        sent_count = 0
        with self.lock:  # Protect connections during iteration
            connections = list(self.incoming_connections.items()) + list(self.outgoing_connections.items())

        for address, sock in connections:
            if exclude_address and address == exclude_address:
                continue
                
            try:
                sock.sendall(framed_data)  # Atomic framed send
                sent_count += 1
                logging.debug(f"Sent broadcast to {address}")
            except (ConnectionError, OSError) as e:
                logging.warning(f"Broadcast failed to {address}: {e}")
                with self.lock:
                    self._cleanup_connection(sock, address)

        # 5. Log results and return
        logging.info(f"Broadcast {message['message_id']} reached {sent_count}/{len(connections)} peers")
        return sent_count

    def _handle_broadcast(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """
        Handle incoming broadcast messages with deduplication.
        Implements controlled flooding with hop limit and message ID tracking.
        """
        # Check for required fields
        if 'message_id' not in message:
            logging.warning("Received broadcast without message ID")
            return

        # Deduplication check
        with self.dedup_lock:
            if message['message_id'] in self.seen_message_ids:
                logging.debug(f"Already seen message {message['message_id']}")
                return
            self.seen_message_ids.add(message['message_id'])

        # Process message locally first
        self._process_received_broadcast(message)

        # Re-broadcast to other peers (excluding sender)
        self.broadcast_message(message, exclude_address=connection)

    def _process_received_broadcast(self, message: dict):
        """Handle a received broadcast message locally"""
        logging.info(f"Received broadcast [{message['message_id']}]: {message.get('content', '')}")
        # Add your application-specific broadcast handling here

    def send_public_message(self, content: str):
        """Helper method to send a public message with automatic ID generation"""
        self.broadcast_message({
            "type": "broadcast",
            "content": content,
            "timestamp": time.time()
        })

    def _handle_port_announce(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Handle port announcement from peers"""
        peer_port = message.get("port")
        if not peer_port:
            logging.error(f"Invalid port announcement from {connection}")
            return

        peer_host = connection[0]
        peer_address = (peer_host, peer_port)
        
        with self.lock:
            if peer_address not in self.known_peers:
                self.known_peers.add(peer_address)
                logging.info(f"Discovered new peer: {peer_address}")

        # Establish reciprocal connection if needed
        if peer_address not in self.outgoing_connections:
            self._establish_reciprocal_connection(peer_host, peer_port)

    def _handle_ping(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Handle ping messages for keep-alive"""
        self.send_message(connection, {"type": "pong", "timestamp": message["timestamp"]})

    def start(self):
        """Start the P2P node server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            logging.info(f"P2P node started on {self.host}:{self.port}")
            
            # Start connection acceptor thread
            acceptor_thread = threading.Thread(target=self._accept_connections, daemon=True)
            acceptor_thread.start()
            self.threads.append(acceptor_thread)
            
        except Exception as e:
            logging.error(f"Failed to start server: {e}")
            self.stop()

    def _accept_connections(self):
        """Accept incoming connections with duplicate check"""
        while self.running:
            try:
                sock, addr = self.server_socket.accept()
                
                with self.lock:
                    if addr in self.incoming_connections:
                        logging.warning(f"Duplicate incoming connection from {addr} rejected")
                        sock.close()
                        continue
                    
                    self.incoming_connections[addr] = sock
                    logging.info(f"New incoming connection from {addr}")
                
                # Start handler thread
                handler_thread = threading.Thread(
                    target=self._handle_connection,
                    args=(sock, addr),
                    daemon=True
                )
                handler_thread.start()
                self.threads.append(handler_thread)
                
            except socket.error as e:
                if self.running:
                    logging.error(f"Connection accept error: {e}")

    def _handle_connection(self, sock: socket.socket, address: Tuple[str, int]):
        """Handle incoming connection lifecycle"""
        try:
            # Read framed messages
            while self.running:
                message_str = FrameProtocol.read_message(sock)
                if not message_str:
                    break
                
                self.message_processor.process_message(message_str, address, self)
                
        except Exception as e:
            logging.error(f"Connection error: {e}")
        finally:
            self._cleanup_connection(sock, address)

    def connect_to_peer(self, peer_host: str, peer_port: int):
        """Initiate outgoing connection to a peer"""
        if (peer_host, peer_port) == (self.host, self.port):
            logging.warning("Cannot connect to self")
            return False

        with self.lock:
            if (peer_host, peer_port) in self.outgoing_connections:
                logging.info(f"Already connected to {peer_host}:{peer_port}")
                return True

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_host, peer_port))
            
            with self.lock:
                self.outgoing_connections[(peer_host, peer_port)] = sock
                self.known_peers.add((peer_host, peer_port))
            
            # Send port announcement immediately
            self.send_message((peer_host, peer_port), {
                "type": "port_announce",
                "port": self.port
            })
            
            # Start handler thread
            handler_thread = threading.Thread(
                target=self._handle_connection,
                args=(sock, (peer_host, peer_port)),
                daemon=True
            )
            handler_thread.start()
            self.threads.append(handler_thread)
            
            logging.info(f"Connected to {peer_host}:{peer_port}")
            return True
            
        except Exception as e:
            logging.error(f"Connection to {peer_host}:{peer_port} failed: {e}")
            return False

    def _establish_reciprocal_connection(self, peer_host: str, peer_port: int):
        """Establish outgoing connection after receiving incoming"""
        if (peer_host, peer_port) in self.outgoing_connections:
            return

        logging.info(f"Attempting reciprocal connection to {peer_host}:{peer_port}")
        self.connect_to_peer(peer_host, peer_port)

    def send_message(self, address: Tuple[str, int], message: dict):
        """Send message to a specific connection"""
        with self.lock:
            sock = self.outgoing_connections.get(address) or \
                   self.incoming_connections.get(address)
        
        if not sock:
            logging.error(f"No connection to {address}")
            return

        try:
            # Convert to framed bytes
            message_str = json.dumps(message)
            framed_data = FrameProtocol.encode(message_str)
            sock.sendall(framed_data)
        except (ConnectionError, json.JSONDecodeError) as e:
            self._cleanup_connection(sock, address)

    def _cleanup_connection(self, sock: socket.socket, address: Tuple[str, int]):
        """Cleanup closed connections"""
        try:
            sock.close()
        except:
            pass

        with self.lock:
            # Remove from connection maps
            if address in self.incoming_connections:
                del self.incoming_connections[address]
                logging.info(f"Removed incoming connection: {address}")
            elif address in self.outgoing_connections:
                del self.outgoing_connections[address]
                logging.info(f"Removed outgoing connection: {address}")

            # Keep in known_peers for potential reconnection
            if address in self.known_peers:
                self.known_peers.remove(address)

    def stop(self):
        """Graceful shutdown"""
        self.running = False
        logging.info("Shutting down node...")
        
        with self.lock:
            # Close all connections
            for sock in list(self.incoming_connections.values()) + list(self.outgoing_connections.values()):
                try:
                    sock.close()
                except:
                    pass
            self.incoming_connections.clear()
            self.outgoing_connections.clear()
            
            # Close server socket
            if self.server_socket:
                try:
                    self.server_socket.close()
                except:
                    pass

        # Wait for threads
        for t in self.threads:
            t.join(timeout=1)

        logging.info("Node shutdown complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Network Server with Message Processing')
    parser.add_argument('--host', default='0.0.0.0', help='Server host')
    parser.add_argument('--port', type=int, default=5000, help='Server port')
    parser.add_argument('--connect', nargs='+', help='Peers to connect to (host:port)')
    args = parser.parse_args()

    server = NetworkServer(args.host, args.port)
    server.start()

    if args.connect:
        for peer in args.connect:
            try:
                peer_host, peer_port = peer.split(':')
                server.connect_to_peer(peer_host, int(peer_port))
            except ValueError:
                logging.error(f"Invalid peer format: {peer}. Use host:port")

    try:
        while True:
            # Server administration could be added here
            time.sleep(5)  # Reduce CPU usage
            if random.random() > 0.5:  # Randomize broadcasts
                server.send_public_message(f"Status update {time.time()}")
    except KeyboardInterrupt:
        server.stop()