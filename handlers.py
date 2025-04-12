import socket
import uuid

class HelloHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_sock: socket.socket) -> bool:
        """Process HELLO messages (peer discovery)"""
        listen_port = message.get('listen_port')
        if not listen_port:
            return False

        # Extract sender IP from socket
        with self.node.connection_lock:
            sender_ip = self.node.inbound_connections.get(sender_sock, ("", 0))[0]

        new_peer = (sender_ip, listen_port)
        with self.node.peer_list_lock:
            if new_peer not in self.node.bootstrap_peers:
                self.node.bootstrap_peers.add(new_peer)
                print(f"Discovered peer: {new_peer}")

        return False  # Do NOT forward HELLO messages

class RequestHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_sock: socket.socket) -> bool:
        """Example request-response handler"""
        response = {
            'type': 'RESPONSE',
            'id': str(uuid.uuid4()),
            'original_id': message['id'],
            'content': 'Here is your response'
        }
        
        # Send direct response through original socket
        self.node._send_direct_message(response, sock=sender_sock)
        return False  # Prevent forwarding

class ResponseHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_sock: socket.socket) -> bool:
        """Example request-response handler"""
        print(f"Received response for request {message['original_id']}")
        return False  # Prevent forwarding