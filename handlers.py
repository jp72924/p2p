import uuid
from typing import Tuple

class HelloHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_addr: Tuple[str, int]) -> bool:
        """Process HELLO messages (peer discovery)"""
        listen_port = message.get('listen_port')
        if not listen_port:
            return False

        sender_ip, _ = sender_addr
        new_peer = (sender_ip, listen_port)
        with self.node.peer_list_lock:
            if new_peer not in self.node.bootstrap_peers:
                self.node.bootstrap_peers.add(new_peer)
                print(f"Discovered peer: {new_peer}")

        return False  # Do NOT forward HELLO messages

class RequestHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_addr: Tuple[str, int]) -> bool:
        """Example request-response handler"""
        response = {
            'type': 'RESPONSE',
            'id': str(uuid.uuid4()),
            'original_id': message['id'],
            'content': 'Here is your response'
        }
        
        # Send direct response through original socket
        self.node._send_direct_message(response, addr=sender_addr)
        return False  # Prevent forwarding

class ResponseHandler:
    def __init__(self, node: 'PeerNode'):
        self.node = node

    def __call__(self, message: dict, sender_addr: Tuple[str, int]) -> bool:
        """Example request-response handler"""
        print(f"Received response for request {message['original_id']}")
        return False  # Prevent forwarding