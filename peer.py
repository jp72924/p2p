import socket
import threading
import logging
from typing import Dict, Tuple, Set, Optional

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

class NetworkServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.threads = []
        
        # Connection tracking
        self.active_connections: Dict[Tuple[str, int], socket.socket] = {}
        self.server_peers: Set[Tuple[str, int]] = set()  # Other servers we connect to
        self.lock = threading.Lock()
        
        # Server identification
        self.server_id = f"{host}:{port}"

    def start(self):
        """Start the server and listen for incoming connections."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            logging.info(f"Server {self.server_id} started and listening")
            self.running = True
            
            # Start acceptor thread
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
            
            # Start handler thread for this peer connection
            peer_thread = threading.Thread(
                target=self._handle_peer_connection,
                args=(peer_socket, (peer_host, peer_port)),
                daemon=True
            )
            peer_thread.start()
            self.threads.append(peer_thread)
            
            logging.info(f"Connected to peer server {peer_host}:{peer_port}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to connect to {peer_host}:{peer_port}: {e}")
            return False

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

    def _broadcast_message(self, message: str, exclude_address: Optional[Tuple[str, int]] = None):
        """Send message to all active connections except the excluded one."""
        with self.lock:
            for address, sock in list(self.active_connections.items()):
                if address == exclude_address:
                    continue
                
                try:
                    formatted_msg = f"[{self.server_id}] {message}\n"
                    sock.sendall(formatted_msg.encode('utf-8'))
                except (ConnectionError, OSError) as e:
                    logging.error(f"Error sending to {address}: {e}")
                    self._cleanup_connection(sock, address)

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
    
    parser = argparse.ArgumentParser(description='Network Server')
    parser.add_argument('--host', default='0.0.0.0', help='Server host')
    parser.add_argument('--port', type=int, default=5000, help='Server port')
    parser.add_argument('--connect', nargs='+', help='Peers to connect to (host:port)')
    args = parser.parse_args()

    server = NetworkServer(args.host, args.port)
    server.start()

    # Connect to peer servers if specified
    if args.connect:
        for peer in args.connect:
            try:
                peer_host, peer_port = peer.split(':')
                server.connect_to_peer(peer_host, int(peer_port))
            except ValueError:
                logging.error(f"Invalid peer format: {peer}. Use host:port")

    try:
        while True:
            # Keep main thread alive
            pass
    except KeyboardInterrupt:
        server.stop()