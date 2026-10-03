"""The actual thing being replicated: a trivial in-memory key-value store.

Raft doesn't care what the state machine does with committed commands -- it
just guarantees every node applies the same commands in the same order. That
means every replica's KVStateMachine ends up in an identical state.
"""

import threading


class KVStateMachine:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def apply(self, command):
        """Called by Raft, in log order, once a command has committed."""
        op = command.get("op")
        with self._lock:
            if op == "PUT":
                self._data[command["key"]] = command["value"]
                return {"ok": True}
            if op == "DELETE":
                self._data.pop(command["key"], None)
                return {"ok": True}
            return {"ok": False, "error": f"unknown op {op!r}"}

    def get(self, key):
        """Local read of current state. Served only by the leader (see
        server.py) -- not itself linearizable-safe against a stale leader,
        but fine for a teaching project. See README for how to harden this."""
        with self._lock:
            return self._data.get(key)
