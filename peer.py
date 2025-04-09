import socket
import threading
import logging

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

    def start(self):
        """Start the TCP server and listen for incoming connections."""
        try:
            # Create a TCP socket
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            # Enable address reuse
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind to the specified host and port
            self.server_socket.bind((self.host, self.port))
            
            # Listen for incoming connections (backlog of 5)
            self.server_socket.listen(5)
            
            logging.info(f"Server started on {self.host}:{self.port}")
            self.running = True
            
            # Main server loop
            while self.running:
                try:
                    # Accept a new connection
                    client_socket, client_address = self.server_socket.accept()
                    logging.info(f"New connection from {client_address}")
                    
                    # Create a new thread to handle the client
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

    def handle_client(self, client_socket, client_address):
        """Handle communication with a connected client."""
        try:
            while self.running:
                # Receive data from the client (max 1024 bytes)
                data = client_socket.recv(1024)
                if not data:
                    break  # Client disconnected
                
                # Process the received data
                message = data.decode('utf-8').strip()
                logging.info(f"Received from {client_address}: {message}")
                
                # Prepare a response (echo back in this example)
                response = f"ECHO: {message}\n"
                client_socket.sendall(response.encode('utf-8'))
                
        except ConnectionResetError:
            logging.info(f"Client {client_address} disconnected abruptly")
        except Exception as e:
            logging.error(f"Error with client {client_address}: {e}")
        finally:
            client_socket.close()
            logging.info(f"Connection with {client_address} closed")

    def stop(self):
        """Stop the server and clean up resources."""
        if not self.running:
            return
            
        self.running = False
        logging.info("Shutting down server...")
        
        # Close the server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logging.error(f"Error closing server socket: {e}")
        
        # Wait for all client threads to finish
        for thread in self.client_threads:
            thread.join(timeout=1)
        
        logging.info("Server shutdown complete")

if __name__ == "__main__":
    # Create and start the server
    server = ThreadedTCPServer(host='0.0.0.0', port=5000)
    
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()