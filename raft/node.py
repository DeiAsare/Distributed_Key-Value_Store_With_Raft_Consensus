"""Core Raft consensus algorithm: leader election + log replication.

This follows the Raft paper (Ongaro & Ousterhout, "In Search of an
Understandable Consensus Algorithm") closely enough to be a faithful teaching
implementation, with two deliberate simplifications called out in the
README: no on-disk persistence, and reads are served from the leader's local
state rather than going through a read-index/lease protocol.
"""

import random
import threading
import time

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"


class RaftNode:
    def __init__(
        self,
        node_id,
        peer_ids,
        transport_call,
        apply_callback,
        election_timeout_range=(0.15, 0.3),
        heartbeat_interval=0.05,
    ):
        """
        node_id:          this node's id (str)
        peer_ids:         list of the OTHER node ids in the cluster
        transport_call:   fn(peer_id, message: dict) -> response dict | None
        apply_callback:   fn(command) -> result, called once a command commits
        """
        self.id = node_id
        self.peer_ids = list(peer_ids)
        self.call = transport_call
        self.apply_callback = apply_callback

        self.lock = threading.RLock()

        # --- persistent state (kept in memory only; see README) ---
        self.current_term = 0
        self.voted_for = None
        # index 0 is a sentinel so log indices match the paper's 1-based scheme
        self.log = [{"term": 0, "command": None}]

        # --- volatile state on all servers ---
        self.commit_index = 0
        self.last_applied = 0
        self.state = FOLLOWER
        self.leader_id = None

        # --- volatile state on leaders (reset on election) ---
        self.next_index = {}
        self.match_index = {}

        self.election_timeout_range = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self.election_deadline = self._new_election_deadline()

        self._running = False

        # lets submit() block until a command it appended has been applied
        self._commit_waiters = {}   # log index -> threading.Event
        self._commit_results = {}   # log index -> result of apply_callback

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def start(self):
        self._running = True
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._apply_loop, daemon=True).start()

    def stop(self):
        self._running = False

    def is_leader(self):
        with self.lock:
            return self.state == LEADER

    def get_leader_hint(self):
        with self.lock:
            return self.leader_id

    def _new_election_deadline(self):
        return time.time() + random.uniform(*self.election_timeout_range)

    def _majority(self):
        return (len(self.peer_ids) + 1) // 2 + 1

    # ------------------------------------------------------------------ #
    # elections
    # ------------------------------------------------------------------ #
    def _election_timer_loop(self):
        while self._running:
            time.sleep(0.01)
            with self.lock:
                due = self.state != LEADER and time.time() >= self.election_deadline
            if due:
                self._start_election()

    def _start_election(self):
        with self.lock:
            self.state = CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            self.leader_id = None
            term = self.current_term
            self.election_deadline = self._new_election_deadline()
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]["term"]

        votes = {self.id}
        votes_lock = threading.Lock()
        won = threading.Event()

        def request_vote(peer_id):
            resp = self.call(peer_id, {
                "type": "RequestVote",
                "term": term,
                "candidate_id": self.id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            })
            if resp is None:
                return
            with self.lock:
                if resp.get("term", 0) > self.current_term:
                    self._become_follower(resp["term"])
                    return
                still_candidate = self.state == CANDIDATE and self.current_term == term
            if still_candidate and resp.get("vote_granted"):
                with votes_lock:
                    votes.add(peer_id)
                    if len(votes) >= self._majority():
                        won.set()

        for pid in self.peer_ids:
            threading.Thread(target=request_vote, args=(pid,), daemon=True).start()

        if len(votes) >= self._majority():
            # single-node cluster: we already won with just our own vote
            self._become_leader(term)
            return

        if won.wait(timeout=self.election_timeout_range[1]):
            self._become_leader(term)

    def _become_follower(self, term):
        self.current_term = term
        self.state = FOLLOWER
        self.voted_for = None
        self.election_deadline = self._new_election_deadline()

    def _become_leader(self, term):
        with self.lock:
            if self.state != CANDIDATE or self.current_term != term:
                return
            self.state = LEADER
            self.leader_id = self.id
            last_index = len(self.log) - 1
            self.next_index = {pid: last_index + 1 for pid in self.peer_ids}
            self.match_index = {pid: 0 for pid in self.peer_ids}
        threading.Thread(target=self._leader_loop, args=(term,), daemon=True).start()

    # ------------------------------------------------------------------ #
    # leader: heartbeats + log replication
    # ------------------------------------------------------------------ #
    def _leader_loop(self, term):
        while self._running:
            with self.lock:
                still_leader = self.state == LEADER and self.current_term == term
            if not still_leader:
                return
            for pid in self.peer_ids:
                threading.Thread(target=self._replicate_to, args=(pid, term), daemon=True).start()
            time.sleep(self.heartbeat_interval)

    def _replicate_to(self, pid, term):
        with self.lock:
            if self.state != LEADER or self.current_term != term:
                return
            next_idx = self.next_index.get(pid, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index]["term"]
            entries = list(self.log[next_idx:])
            leader_commit = self.commit_index

        resp = self.call(pid, {
            "type": "AppendEntries",
            "term": term,
            "leader_id": self.id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": leader_commit,
        })
        if resp is None:
            return  # peer unreachable; we'll retry on the next heartbeat

        with self.lock:
            if resp.get("term", 0) > self.current_term:
                self._become_follower(resp["term"])
                return
            if self.state != LEADER or self.current_term != term:
                return
            if resp.get("success"):
                self.match_index[pid] = prev_log_index + len(entries)
                self.next_index[pid] = self.match_index[pid] + 1
                self._advance_commit_index()
            else:
                # simple back-off; a real system would jump back a whole
                # conflicting term at once, this just walks back one entry
                self.next_index[pid] = max(1, self.next_index.get(pid, 1) - 1)

    def _advance_commit_index(self):
        """Raft only ever commits by counting replicas of an entry from the
        CURRENT term (committing older-term entries directly is unsafe)."""
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n]["term"] != self.current_term:
                continue
            count = 1  # ourselves
            for pid in self.peer_ids:
                if self.match_index.get(pid, 0) >= n:
                    count += 1
            if count >= self._majority():
                self.commit_index = n
                break

    # ------------------------------------------------------------------ #
    # RPC handlers - invoked by the server on incoming messages
    # ------------------------------------------------------------------ #
    def handle_request_vote(self, msg):
        with self.lock:
            term = msg["term"]
            if term > self.current_term:
                self._become_follower(term)
            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index]["term"]
            log_is_ok = (
                msg["last_log_term"] > last_log_term
                or (msg["last_log_term"] == last_log_term and msg["last_log_index"] >= last_log_index)
            )

            if self.voted_for in (None, msg["candidate_id"]) and log_is_ok:
                self.voted_for = msg["candidate_id"]
                self.election_deadline = self._new_election_deadline()
                return {"term": self.current_term, "vote_granted": True}
            return {"term": self.current_term, "vote_granted": False}

    def handle_append_entries(self, msg):
        with self.lock:
            term = msg["term"]
            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term:
                self._become_follower(term)
            # any valid AppendEntries (even same-term) means: step down / stay follower
            self.state = FOLLOWER
            self.leader_id = msg["leader_id"]
            self.election_deadline = self._new_election_deadline()

            prev_index = msg["prev_log_index"]
            prev_term = msg["prev_log_term"]
            if prev_index >= len(self.log) or self.log[prev_index]["term"] != prev_term:
                return {"term": self.current_term, "success": False}

            insert_at = prev_index + 1
            for i, entry in enumerate(msg["entries"]):
                idx = insert_at + i
                if idx < len(self.log):
                    if self.log[idx]["term"] != entry["term"]:
                        self.log = self.log[:idx] + [entry]
                else:
                    self.log.append(entry)

            if msg["leader_commit"] > self.commit_index:
                self.commit_index = min(msg["leader_commit"], len(self.log) - 1)

            return {"term": self.current_term, "success": True}

    # ------------------------------------------------------------------ #
    # client-facing: append a new command and wait for it to commit
    # ------------------------------------------------------------------ #
    def submit(self, command, timeout=2.0):
        """Returns (True, result) on success, or (False, leader_id_or_'timeout')."""
        with self.lock:
            if self.state != LEADER:
                return False, self.leader_id
            index = len(self.log)
            self.log.append({"term": self.current_term, "command": command})
            event = threading.Event()
            self._commit_waiters[index] = event

        if event.wait(timeout):
            with self.lock:
                result = self._commit_results.pop(index, None)
                self._commit_waiters.pop(index, None)
            return True, result

        with self.lock:
            self._commit_waiters.pop(index, None)
        return False, "timeout"

    def _apply_loop(self):
        while self._running:
            time.sleep(0.01)
            with self.lock:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.log[self.last_applied]
                    result = None
                    if entry["command"] is not None:
                        result = self.apply_callback(entry["command"])
                    waiter = self._commit_waiters.get(self.last_applied)
                    if waiter is not None:
                        self._commit_results[self.last_applied] = result
                        waiter.set()
