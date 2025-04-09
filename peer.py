import socket
import threading
import logging
import json
from typing import Dict, Tuple, Set, Optional, Callable

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

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
                raise ValueError("Message must be a JSON object")
            
            msg_type = msg_data.get("type", "default")
            handler = self.handlers.get(msg_type, self.default_handler)
            return handler(msg_data, connection, server)
        except json.JSONDecodeError:
            return self.default_handler({"content": message}, connection, server)
        except Exception as e:
            logging.error(f"Message processing error: {e}")
            return None

    def _handle_default_message(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Default handler for unregistered message types"""
        content = message.get("content", "")
        if content:
            server._broadcast_message({
                "type": "broadcast",
                "from": connection,
                "content": content
            }, exclude_address=connection)
        return None

class NetworkServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.threads = []
        
        # Connection tracking
        self.active_connections: Dict[Tuple[str, int], socket.socket] = {}
        self.server_peers: Set[Tuple[str, int]] = set()
        self.lock = threading.Lock()
        
        # Server identification
        self.server_id = f"{host}:{port}"
        
        # Message processing
        self.message_processor = MessageProcessor()
        self._register_default_handlers()

    def _register_default_handlers(self):
        """Register default message handlers"""
        self.message_processor.register_handler("connect", self._handle_connect_message)
        self.message_processor.register_handler("status", self._handle_status_request)
        self.message_processor.register_handler("broadcast", self._handle_broadcast_message)

    def _handle_connect_message(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Handle connection request to another server"""
        peer_host = message.get("host")
        peer_port = message.get("port")
        if peer_host and peer_port:
            self.connect_to_peer(peer_host, peer_port)

    def _handle_status_request(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Handle status request and return connection info"""
        with self.lock:
            status = {
                "type": "status_response",
                "server": self.server_id,
                "connections": len(self.active_connections),
                "peers": [f"{host}:{port}" for host, port in self.server_peers]
            }
        self.send_message(connection, status)

    def _handle_broadcast_message(self, message: dict, connection: Tuple[str, int], server: 'NetworkServer'):
        """Handle broadcast messages"""
        content = message.get("content", "")
        if content:
            logging.info(f"Broadcast from {connection}: {content}")
            # Re-broadcast to all other connections
            message["hops"] = message.get("hops", 0) + 1
            self._broadcast_message(message, exclude_address=connection)

    def start(self):
        """Start the server and listen for incoming connections."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            logging.info(f"Server {self.server_id} started with message processing")
            self.running = True
            
            acceptor_thread = threading.Thread(
                target=self._accept_connections,
                daemon=True
            )
            acceptor_thread.start()
            self.threads.append(acceptor_thread)
            
        except Exception as e:
            logging.error(f"Failed to start server: {e}")
            self.stop()

    def connect_to_peer(self, peer_host: str, peer_port: int):
        """Establish connection to another server."""
        if (peer_host, peer_port) == (self.host, self.port):
            logging.warning("Cannot connect to self")
            return False
            
        with self.lock:
            if (peer_host, peer_port) in self.server_peers:
                logging.info(f"Already connected to {peer_host}:{peer_port}")
                return True

        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.connect((peer_host, peer_port))
            
            with self.lock:
                self.active_connections[(peer_host, peer_port)] = peer_socket
                self.server_peers.add((peer_host, peer_port))
            
            peer_thread = threading.Thread(
                target=self._handle_peer_connection,
                args=(peer_socket, (peer_host, peer_port)),
                daemon=True
            )
            peer_thread.start()
            self.threads.append(peer_thread)
            
            logging.info(f"Connected to peer server {peer_host}:{peer_port}")
            
            # Send connection announcement
            self.send_message((peer_host, peer_port), {
                "type": "peer_connect",
                "host": self.host,
                "port": self.port
            })
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to {peer_host}:{peer_port}: {e}")
            return False

    def send_message(self, address: Tuple[str, int], message: dict):
        """Send a structured message to a specific connection."""
        with self.lock:
            sock = self.active_connections.get(address)
        
        if sock:
            try:
                sock.sendall((json.dumps(message) + "\n").encode('utf-8'))
            except (ConnectionError, OSError) as e:
                logging.error(f"Error sending to {address}: {e}")
                self._cleanup_connection(sock, address)

    def _broadcast_message(self, message: dict, exclude_address: Optional[Tuple[str, int]] = None):
        """Send message to all active connections except the excluded one."""
        with self.lock:
            connections = list(self.active_connections.items())
        
        for address, sock in connections:
            if address == exclude_address:
                continue
            
            try:
                sock.sendall((json.dumps(message) + "\n").encode('utf-8'))
            except (ConnectionError, OSError) as e:
                logging.error(f"Error broadcasting to {address}: {e}")
                self._cleanup_connection(sock, address)

    def _accept_connections(self):
        """Accept incoming connections (both clients and servers)."""
        while self.running:
            try:
                client_socket, client_address = self.server_socket.accept()
                
                with self.lock:
                    self.active_connections[client_address] = client_socket
                
                # Start handler thread
                handler_thread = threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_socket, client_address),
                    daemon=True
                )
                handler_thread.start()
                self.threads.append(handler_thread)
                
                logging.info(f"New connection from {client_address}")
                
            except socket.error as e:
                if self.running:
                    logging.error(f"Error accepting connection: {e}")

    def _handle_incoming_connection(self, sock: socket.socket, address: Tuple[str, int]):
        """Handle incoming connection (could be client or server)."""
        try:
            while self.running:
                data = sock.recv(1024)
                if not data:
                    break
                
                message = data.decode('utf-8').strip()
                logging.info(f"Received from {address}: {message}")
                
                # Broadcast to all connections except sender
                self._broadcast_message(message, exclude_address=address)
                
        except ConnectionResetError:
            logging.info(f"Connection with {address} reset")
        except Exception as e:
            logging.error(f"Error handling {address}: {e}")
        finally:
            self._cleanup_connection(sock, address)

    def _handle_peer_connection(self, sock: socket.socket, peer_address: Tuple[str, int]):
        """Handle established connection to another server."""
        try:
            while self.running:
                data = sock.recv(1024)
                if not data:
                    break
                
                message = data.decode('utf-8').strip()
                logging.info(f"Received from peer {peer_address}: {message}")
                
                # Broadcast to all other connections
                self._broadcast_message(message, exclude_address=peer_address)
                
        except Exception as e:
            logging.error(f"Peer connection error {peer_address}: {e}")
        finally:
            self._cleanup_connection(sock, peer_address)

    def _cleanup_connection(self, sock: socket.socket, address: Tuple[str, int]):
        """Clean up a disconnected connection."""
        try:
            sock.close()
        except:
            pass
            
        with self.lock:
            if address in self.active_connections:
                del self.active_connections[address]
            if address in self.server_peers:
                self.server_peers.remove(address)
        
        logging.info(f"Connection closed: {address}")

    def stop(self):
        """Stop the server and clean up resources."""
        if not self.running:
            return
            
        self.running = False
        logging.info("Shutting down server...")
        
        # Close all connections
        with self.lock:
            for sock in self.active_connections.values():
                try:
                    sock.close()
                except:
                    pass
            self.active_connections.clear()
            self.server_peers.clear()
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logging.error(f"Error closing server socket: {e}")
        
        # Wait for threads
        for thread in self.threads:
            thread.join(timeout=1)
        
        logging.info("Server shutdown complete")


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
            pass
    except KeyboardInterrupt:
        server.stop()