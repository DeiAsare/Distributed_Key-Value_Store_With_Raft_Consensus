"""End-to-end test: a real 3-node cluster talking over real TCP sockets on
localhost (each node is a genuine KVServer with its own RPCServer and
RaftNode running in background threads -- this is not a mock).

Run with:  python3 -m unittest tests.test_cluster -v
"""

import time
import unittest

from raft import rpc
from raft.server import KVServer

BASE_PORT = 9500  # distinct from cluster.json's ports so this can run alongside a manual demo


def make_cluster(node_ids, base_port=BASE_PORT):
    addrs = {nid: ("127.0.0.1", base_port + i) for i, nid in enumerate(node_ids)}
    servers = {}
    for nid in node_ids:
        peers = {other: addr for other, addr in addrs.items() if other != nid}
        host, port = addrs[nid]
        servers[nid] = KVServer(nid, host, port, peers)
    return servers, addrs


def ping(addr, timeout=0.3):
    return rpc.call(addr[0], addr[1], {"type": "Ping"}, timeout=timeout)


def wait_for_leader(addrs, timeout=3.0):
    """Poll every node until exactly one reports itself as leader; return its id."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for nid, addr in addrs.items():
            resp = ping(addr)
            if resp and resp.get("success") and resp.get("leader"):
                return nid
        time.sleep(0.02)
    return None


def client_call(addrs, message, tries=25, retry_delay=0.1):
    """A tiny stand-in for kvctl's retry logic, for use inside the test."""
    node_ids = list(addrs.keys())
    target = 0
    for _ in range(tries):
        nid = node_ids[target]
        resp = rpc.call(addrs[nid][0], addrs[nid][1], message, timeout=0.5)
        if resp is None:
            target = (target + 1) % len(node_ids)
            time.sleep(retry_delay)
            continue
        if resp.get("success"):
            return resp
        hint = resp.get("leader_hint")
        if hint and hint in node_ids:
            target = node_ids.index(hint)
        else:
            target = (target + 1) % len(node_ids)
        time.sleep(retry_delay)
    raise AssertionError(f"no leader found after {tries} attempts")


class TestRaftCluster(unittest.TestCase):
    def setUp(self):
        self.node_ids = ["node1", "node2", "node3"]
        self.servers, self.addrs = make_cluster(self.node_ids)
        for s in self.servers.values():
            s.start()

    def tearDown(self):
        for s in self.servers.values():
            s.stop()

    def test_election_replication_and_failover(self):
        # 1. A leader should emerge on its own.
        leader_id = wait_for_leader(self.addrs)
        self.assertIsNotNone(leader_id, "no leader elected within timeout")

        # 2. A write through any node should succeed and be readable back.
        resp = client_call(self.addrs, {"type": "ClientPut", "key": "name", "value": "alice"})
        self.assertTrue(resp["success"])

        resp = client_call(self.addrs, {"type": "ClientGet", "key": "name"})
        self.assertTrue(resp["success"])
        self.assertEqual(resp["value"], "alice")

        # 3. Kill the leader; the remaining two nodes must elect a new one.
        self.servers[leader_id].stop()
        remaining_addrs = {nid: a for nid, a in self.addrs.items() if nid != leader_id}
        new_leader_id = wait_for_leader(remaining_addrs)
        self.assertIsNotNone(new_leader_id, "no new leader elected after failover")
        self.assertNotEqual(new_leader_id, leader_id)

        # 4. The cluster must still be usable, and old data must have survived.
        resp = client_call(remaining_addrs, {"type": "ClientPut", "key": "city", "value": "accra"})
        self.assertTrue(resp["success"])

        resp = client_call(remaining_addrs, {"type": "ClientGet", "key": "name"})
        self.assertTrue(resp["success"])
        self.assertEqual(resp["value"], "alice", "old data lost after leader failover")

        resp = client_call(remaining_addrs, {"type": "ClientGet", "key": "city"})
        self.assertTrue(resp["success"])
        self.assertEqual(resp["value"], "accra")

        # 5. Delete should replicate too.
        resp = client_call(remaining_addrs, {"type": "ClientDelete", "key": "name"})
        self.assertTrue(resp["success"])
        resp = client_call(remaining_addrs, {"type": "ClientGet", "key": "name"})
        self.assertIsNone(resp["value"])


if __name__ == "__main__":
    unittest.main()
