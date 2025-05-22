import socket
from typing import List, Dict, Callable, Tuple


class MessageRouter:
    def __init__(self, node: 'PeerNode'):
        self.node = node
        self.handlers: Dict[str, Callable] = {}
        self.middleware: List[Callable] = []
        self.default_handler = self._forward_message

    def add_handler(self, message_type: str, handler: Callable):
        """Register handler for a specific message type"""
        with self.node.message_dedup_lock:
            self.handlers[message_type] = handler

    def add_middleware(self, middleware_func: Callable):
        """Add pre-processing step (e.g., validation, logging)"""
        self.middleware.append(middleware_func)

    def route_message(self, message: dict, sender_addr: Tuple[str, int]):
        """Process incoming message through pipeline"""
        try:
            # Deserialize and deduplicate
            msg_id = message.get('id')
            msg_type = message.get('type', 'unknown')

            # Deduplication check
            with self.node.message_dedup_lock:
                if msg_id in self.node.seen_messages:
                    return
                self.node.seen_messages.add(msg_id)

            # Apply middleware (e.g., logging, validation)
            for middleware in self.middleware:
                message = middleware(message) or message

            # Route to handler or forward
            handler = self.handlers.get(msg_type, self.default_handler)
            should_forward = handler(message, sender_addr)
            
            # Forward if handler allows
            if should_forward:
                self._forward_message(message, sender_addr)

        except json.JSONDecodeError:
            print(f"Malformed message: {message}")

    def _forward_message(self, message: dict, exclude_addr: Tuple[str, int]) -> bool:
        """Default handler: forward message to all peers except sender"""
        self.node._broadcast_message(message, exclude_addr)
        return False  # Prevent re-forwarding loops