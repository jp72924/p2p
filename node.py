import json
import socket
import struct
import threading
import queue
import time
import uuid
from itertools import chain
from typing import List, Tuple, Dict, Set

class PeerNode:
    def __init__(self, host: str, port: int, bootstrap_peers: list, node_id: str = None):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self.host = host
        self.port = port
        
        # Convert to set for O(1) lookups
        self.bootstrap_peers: Set[Tuple[str, int]] = set(bootstrap_peers)
        self.peer_list_lock = threading.Lock()
        
        # Connection tracking
        self.inbound_connections = {}
        self.outbound_connections = {}
        self.connection_lock = threading.Lock()
        
        # System control
        self.running = True
        self.seen_messages = set()
        self.message_dedup_lock = threading.Lock()

        self.message_inbox = queue.Queue()
        self.message_outbox = queue.Queue()

        # Start core threads
        threading.Thread(target=self._listen_for_peers, daemon=True).start()
        threading.Thread(target=self._manage_peer_connections, daemon=True).start()
        threading.Thread(target=self._handle_peer_messages, daemon=True).start()
        threading.Thread(target=self._dispatch_queued_messages, daemon=True).start()

    # --- Core Node Operations ---
    def send_message(self, message: dict):
        self.message_outbox.put(message)
        print(f"[{self.node_id}] Originating new message: {message.get('type', 'unknown')}")

    def shutdown(self):
        self.running = False

        with self.connection_lock:
            for sock in list(self.inbound_connections.keys()) + list(self.outbound_connections.keys()):
                try:
                    sock.close()
                except Exception:
                    pass

            self.inbound_connections.clear()
            self.outbound_connections.clear()

        print(f"[{self.node_id}] Node shutdown complete")

    def get_connection_stats(self) -> dict:
        with self.connection_lock:
            return {
                'incoming': len(self.inbound_connections),
                'outgoing': len(self.outbound_connections),
                'total': len(self.inbound_connections) + len(self.outbound_connections)
            }

    # --- Thread Workers (Core Logic) ---
    def _listen_for_peers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()

            print(f"[{self.node_id}] Listening for incoming connections...")

            while self.running:
                try:
                    client, addr = server.accept()
                    self._register_peer(client, addr, connection_type="incoming")
                except Exception as e:
                    print(f"[{self.node_id}] Server error: {e}")

    def _manage_peer_connections(self):
        while self.running:
            with self.peer_list_lock:
                current_peers = list(self.bootstrap_peers)
                
            for peer in current_peers:
                if self._can_connect_to_peer(peer):
                    self._connect_to_peer(peer)
            time.sleep(5)

    def _handle_peer_messages(self):
        while self.running:
            try:
                message, sender_sock, conn_type = self.message_inbox.get(timeout=1)
                
                # Message deduplication
                msg_id = message.get('id')
                if not msg_id:
                    continue
                    
                with self.message_dedup_lock:
                    if msg_id in self.seen_messages:
                        continue
                    self.seen_messages.add(msg_id)

                # Handle HELLO protocol
                if message.get('type') == "HELLO":
                    listen_port = message.get('listen_port')
                    if not listen_port:
                        continue

                    # Get sender's IP from connection info
                    with self.connection_lock:
                        if conn_type == "incoming":
                            sender_ip = self.inbound_connections.get(sender_sock, ("", 0))[0]
                        else:
                            sender_ip = self.outbound_connections.get(sender_sock, ("", 0))[0]

                    new_peer = (sender_ip, listen_port)
                    
                    # Add to known peers if not present
                    with self.peer_list_lock:
                        if new_peer not in self.bootstrap_peers:
                            self.bootstrap_peers.add(new_peer)
                            print(f"[{self.node_id}] Discovered new peer: {new_peer[0]}:{new_peer[1]}")

                # Forward other messages
                else:
                    forward_count = self._broadcast_message(message, exclude_sock=sender_sock)
                    print(f"[{self.node_id}] Forwarded message {msg_id[:8]} to {forward_count} peers")

            except queue.Empty:
                continue
            except Exception as e:
                print(f"[{self.node_id}] Message processing error: {e}")

    def _dispatch_queued_messages(self):
        while self.running:
            try:
                message = self.message_outbox.get(timeout=1)
                if 'id' not in message:
                    message['id'] = str(uuid.uuid4())
                self._broadcast_message(message)
            except queue.Empty:
                continue

    # --- Networking Helpers ---
    def _connect_to_peer(self, peer: Tuple[str, int]):
        host, port = peer
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            
            # Send HELLO immediately after connecting
            handshake_message = {
                "type": "HELLO",
                "listen_port": self.port,
                "id": str(uuid.uuid4())
            }
            data = json.dumps(handshake_message).encode()
            header = struct.pack('>I', len(data))
            sock.sendall(header + data)
            
            self._register_peer(sock, (host, port), "outgoing")
            print(f"[{self.node_id}] Sent HELLO to {host}:{port}")
        except Exception as e:
            print(f"[{self.node_id}] Connection to {host}:{port} failed: {e}")

    def _broadcast_message(self, message: dict, exclude_sock: socket.socket = None) -> int:
        data = json.dumps(message).encode()
        header = struct.pack('>I', len(data))
        full_message = header + data
        count = 0

        with self.connection_lock:
            all_connections = chain(self.inbound_connections.items(),
                                    self.outbound_connections.items())

            to_remove = []
            for sock, addr in all_connections:
                if sock == exclude_sock:
                    continue

                try:
                    sock.sendall(full_message)
                    count += 1
                except Exception as e:
                    print(f"[{self.node_id}] Send error to {addr[0]}:{addr[1]}: {e}")
                    conn_type = 'incoming' if sock in self.inbound_connections else 'outgoing'
                    to_remove.append((sock, conn_type))

            for sock, conn_type in to_remove:
                self._unregister_peer(sock, conn_type)

        return count

    # --- Connection Management Helpers ---
    def _register_peer(self, sock: socket.socket, address: Tuple[str, int], connection_type: str):
        with self.connection_lock:
            if connection_type == "incoming":
                self.inbound_connections[sock] = address
            else:
                self.outbound_connections[sock] = address

        threading.Thread(
            target=self._handle_connection,
            args=(sock, connection_type),
            daemon=True
        ).start()

        print(f"[{self.node_id}] New {connection_type} connection to {address[0]}:{address[1]}")

    def _unregister_peer(self, sock: socket.socket, connection_type: str):
        with self.connection_lock:
            connections = (self.inbound_connections if connection_type == "incoming"
                else self.outbound_connections)

            if sock in connections:
                addr = connections.pop(sock)
                print(f"[{self.node_id}] {connection_type.capitalize()} connection closed: {addr[0]}:{addr[1]}")
                try:
                    sock.close()
                except Exception:
                    pass

    def _handle_connection(self, sock: socket.socket, connection_type: str):
        while self.running:
            try:
                header = self._receive_exact_bytes(sock, 4)
                if not header:
                    break

                length = struct.unpack('>I', header)[0]
                data = self._receive_exact_bytes(sock, length)
                if not data:
                    break

                message = json.loads(data.decode())
                self.message_inbox.put((message, sock, connection_type))

            except Exception as e:
                print(f"[{self.node_id}] Receive error: {e}")
                break

        self._unregister_peer(sock, connection_type)

    # --- Utility Functions ---
    def _can_connect_to_peer(self, peer: Tuple[str, int]) -> bool:
        host, port = peer
        if (host, port) == (self.host, self.port):
            return False

        with self.connection_lock:
            all_remotes = chain(
                self.inbound_connections.values(),
                self.outbound_connections.values()
            )
            return not any(remote == (host, port) for remote in all_remotes)

    def _receive_exact_bytes(self, sock: socket.socket, n: int) -> bytes:
        data = bytearray()
        while len(data) < n and self.running:
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    return b''
                data.extend(chunk)
            except BlockingIOError:
                continue
            except Exception:
                return b''
        return bytes(data)


if __name__ == "__main__":
    node1 = PeerNode('localhost', 6000, [('127.0.0.1', 6001)], "NODE-A")
    node2 = PeerNode('localhost', 6001, [], "NODE-B")

    time.sleep(2)

    try:
        while True:
            time.sleep(5)
            print("\nCurrent Connection Stats:")
            print(f"Node1: {node1.get_connection_stats()}")
            print(f"Node2: {node2.get_connection_stats()}")

    except KeyboardInterrupt:
        node1.shutdown()
        node2.shutdown()
        print("Network shutdown complete")