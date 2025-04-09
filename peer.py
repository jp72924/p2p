import socket
import threading
import logging
from typing import Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class ThreadedTCPServer:
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
        self.client_threads = []
        self.active_connections: Dict[Tuple[str, int], socket.socket] = {}
        self.lock = threading.Lock()

    def start(self):
        """Start the TCP server and listen for incoming connections."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            logging.info(f"Server started on {self.host}:{self.port}")
            self.running = True
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    logging.info(f"New connection from {client_address}")
                    
                    # Add to active connections
                    with self.lock:
                        self.active_connections[client_address] = client_socket
                    
                    # Start client thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()
                    self.client_threads.append(client_thread)
                    
                except socket.error as e:
                    if self.running:
                        logging.error(f"Error accepting connection: {e}")
            
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            self.stop()

    def handle_client(self, client_socket: socket.socket, client_address: Tuple[str, int]):
        """Handle communication with a connected client."""
        try:
            while self.running:
                data = client_socket.recv(1024)
                if not data:
                    break  # Client disconnected
                
                message = data.decode('utf-8').strip()
                logging.info(f"Received from {client_address}: {message}")
                
                # Broadcast message to all other clients
                self.broadcast_message(message, exclude_client=client_address)
                
        except ConnectionResetError:
            logging.info(f"Client {client_address} disconnected abruptly")
        except Exception as e:
            logging.error(f"Error with client {client_address}: {e}")
        finally:
            # Remove from active connections
            with self.lock:
                if client_address in self.active_connections:
                    del self.active_connections[client_address]
            
            client_socket.close()
            logging.info(f"Connection with {client_address} closed")

    def broadcast_message(self, message: str, exclude_client: Tuple[str, int] = None):
        """Send a message to all connected clients except the excluded one."""
        with self.lock:
            for address, sock in list(self.active_connections.items()):
                if address == exclude_client:
                    continue
                
                try:
                    formatted_message = f"[BROADCAST] {message}\n"
                    sock.sendall(formatted_message.encode('utf-8'))
                except (ConnectionError, OSError) as e:
                    logging.error(f"Error sending to {address}: {e}")
                    # Remove disconnected client
                    del self.active_connections[address]
                    try:
                        sock.close()
                    except:
                        pass

    def stop(self):
        """Stop the server and clean up resources."""
        if not self.running:
            return
            
        self.running = False
        logging.info("Shutting down server...")
        
        # Close all client connections
        with self.lock:
            for sock in self.active_connections.values():
                try:
                    sock.close()
                except:
                    pass
            self.active_connections.clear()
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logging.error(f"Error closing server socket: {e}")
        
        # Wait for client threads
        for thread in self.client_threads:
            thread.join(timeout=1)
        
        logging.info("Server shutdown complete")

if __name__ == "__main__":
    server = ThreadedTCPServer(host='0.0.0.0', port=5000)
    
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()