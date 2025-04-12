import json
import socket
import threading
import queue
import time
import uuid

from itertools import chain
from typing import Tuple
from router import MessageRouter
from protocols import MessageFramer
from handlers import HelloHandler
from handlers import RequestHandler
from handlers import ResponseHandler


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

        self.router = MessageRouter(self)
        self.router.add_handler("HELLO", HelloHandler(self))
        self.router.add_handler("REQUEST", RequestHandler(self))
        self.router.add_handler("RESPONSE", ResponseHandler(self))

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
                raw_msg, sender_sock, _ = self.message_inbox.get(timeout=1)
                self.router.route_message(raw_msg, sender_sock)
            except queue.Empty:
                continue

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
            framed = MessageFramer.frame_message(data)
            sock.sendall(framed)
            
            self._register_peer(sock, (host, port), "outgoing")
            print(f"[{self.node_id}] Sent HELLO to {host}:{port}")
        except Exception as e:
            print(f"[{self.node_id}] Connection to {host}:{port} failed: {e}")

    def _broadcast_message(self, message: dict, exclude_sock: socket.socket = None) -> int:
        data = json.dumps(message).encode()
        full_message = MessageFramer.frame_message(data)
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

    def _send_direct_message(self, message: dict, sock: socket.socket):
        """Send message to a specific peer"""
        data = json.dumps(message).encode()
        framed = MessageFramer.frame_message(data)
        
        with self.connection_lock:  # Ensure thread-safe socket access
            try:
                sock.sendall(framed)
            except Exception as e:
                print(f"[{self.node_id}] Send error: {e}")
                # Determine connection type for cleanup
                if sock in self.inbound_connections:
                    self._unregister_peer(sock, 'incoming')
                elif sock in self.outbound_connections:
                    self._unregister_peer(sock, 'outgoing')

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
                payload = MessageFramer.recv_message(sock)
                if payload:
                    message = json.loads(payload.decode())
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


if __name__ == "__main__":
    node1 = PeerNode('localhost', 6000, [('127.0.0.1', 6001)], "NODE-A")
    node2 = PeerNode('localhost', 6001, [], "NODE-B")
    node3 = PeerNode('localhost', 6002, [('127.0.0.1', 6000)], "NODE-B")

    time.sleep(2)

    try:
        while True:
            time.sleep(5)
            print("\nCurrent Connection Stats:")
            print(f"Node1: {node1.get_connection_stats()}")
            print(f"Node2: {node2.get_connection_stats()}")
            print(f"Node3: {node3.get_connection_stats()}")
            node1.send_message({'type': 'REQUEST', 'content': 'Need data'})
    except KeyboardInterrupt:
        node1.shutdown()
        node2.shutdown()
        print("Network shutdown complete")