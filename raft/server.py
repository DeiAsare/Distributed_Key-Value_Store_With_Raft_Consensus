"""A single cluster node: a Raft consensus instance plus a KV state machine,
exposed over one TCP port that carries both Raft-internal RPCs
(RequestVote/AppendEntries) and client requests (Get/Put/Delete).
"""

from . import rpc
from .node import RaftNode
from .state_machine import KVStateMachine


class KVServer:
    def __init__(self, node_id, host, port, peers):
        """peers: dict of {other_node_id: (host, port)}, excluding this node."""
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers

        self.state_machine = KVStateMachine()
        self.node = RaftNode(node_id, list(peers.keys()), self._transport_call, self.state_machine.apply)
        self.rpc_server = rpc.RPCServer(host, port, self._dispatch)

    def start(self):
        self.rpc_server.start()
        self.node.start()

    def stop(self):
        self.node.stop()
        self.rpc_server.stop()

    def _transport_call(self, peer_id, message):
        peer_host, peer_port = self.peers[peer_id]
        return rpc.call(peer_host, peer_port, message, timeout=0.3)

    def _dispatch(self, msg):
        msg_type = msg.get("type")

        if msg_type == "RequestVote":
            return self.node.handle_request_vote(msg)

        if msg_type == "AppendEntries":
            return self.node.handle_append_entries(msg)

        if msg_type == "ClientPut":
            ok, result = self.node.submit({"op": "PUT", "key": msg["key"], "value": msg["value"]})
            return self._client_response(ok, result)

        if msg_type == "ClientDelete":
            ok, result = self.node.submit({"op": "DELETE", "key": msg["key"]})
            return self._client_response(ok, result)

        if msg_type == "ClientGet":
            if not self.node.is_leader():
                return {"success": False, "leader_hint": self.node.get_leader_hint()}
            return {"success": True, "value": self.state_machine.get(msg["key"])}

        if msg_type == "Ping":
            return {"success": True, "leader": self.node.is_leader(), "term": self.node.current_term}

        return {"success": False, "error": f"unknown message type {msg_type!r}"}

    @staticmethod
    def _client_response(ok, result):
        if ok:
            return {"success": True, "result": result}
        if result == "timeout":
            return {"success": False, "error": "timed out waiting for the entry to commit"}
        return {"success": False, "leader_hint": result}  # result is a node id or None
