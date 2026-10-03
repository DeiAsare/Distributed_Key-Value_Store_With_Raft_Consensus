# raftkv

A distributed, replicated key-value store built from scratch in Python, using
the [Raft consensus algorithm](https://raft.github.io/) to keep multiple
nodes in agreement about the state of the store — including surviving the
leader crashing outright.

No frameworks, no external dependencies: the networking, the wire protocol,
and the consensus algorithm are all hand-rolled, on top of nothing but the
Python standard library (`socket`, `threading`, `json`).

## Why this project

Most CRUD apps don't need to think about consensus. But the moment you run
more than one copy of something and need them to agree on shared state —
replicated databases, leader election in a cluster, distributed locks — you
need an algorithm like Raft (or Paxos). This project implements the core of
Raft end to end: leader election, log replication, and commit/apply
semantics, wired up to a real (if tiny) key-value store you can actually put
data into.

## How it works

```
        ┌────────┐   RequestVote/AppendEntries   ┌────────┐
        │ node1  │ ◄───────────────────────────► │ node2  │
        │ Raft + │ ◄───────────────────────────► │ Raft + │
        │  KV    │                                │  KV    │
        └────┬───┘                                └────┬───┘
             │              RequestVote/AppendEntries   │
             └─────────────────────┬──────────────────┘
                                    ▼
                               ┌────────┐
                               │ node3  │
                               │ Raft + │
                               │  KV    │
                               └────────┘
                 clients (kvctl) can talk to ANY node —
                 non-leaders redirect them to the leader
```

- **Wire protocol** (`raft/rpc.py`): every RPC is a 4-byte length prefix
  followed by a JSON payload over a plain TCP socket. One request/response
  per connection — simple to reason about, at the cost of a bit of
  handshake overhead.
- **Consensus** (`raft/node.py`): a faithful (if simplified) implementation
  of Raft — randomized election timeouts, majority-vote leader election,
  log replication with the standard consistency checks, and the
  current-term-only commit rule that makes Raft safe.
- **State machine** (`raft/state_machine.py`): a trivial `dict`-backed KV
  store. Raft doesn't know or care what it's replicating — it just
  guarantees every node applies the same commands in the same order.
- **Server** (`raft/server.py`): wires the two together and exposes
  `Get`/`Put`/`Delete` over the same port as the Raft-internal RPCs.
- **Client** (`kvctl.py`): a CLI that doesn't need to know who the leader
  is — it just follows `leader_hint` redirects until it finds one.

## Running it

Requires Python 3.8+ and nothing else.

Start a 3-node cluster (3 separate terminals, or use the helper script):

```bash
python3 kvserver.py --id node1 --config cluster.json
python3 kvserver.py --id node2 --config cluster.json
python3 kvserver.py --id node3 --config cluster.json

# or, all at once, backgrounded:
./run_demo.sh
```

Talk to it from any other terminal:

```bash
python3 kvctl.py --config cluster.json put name alice
python3 kvctl.py --config cluster.json get name
python3 kvctl.py --config cluster.json delete name
```

Try killing whichever node is currently the leader (`Ctrl+C` in its
terminal, or `kill <pid>`) and running another `put`/`get` — the remaining
two nodes elect a new leader within a few hundred milliseconds and the
store keeps working, with no data loss.

## Testing

```bash
python3 -m unittest tests.test_cluster -v
```

This isn't a mock-based unit test — it spins up three real `KVServer`
instances talking over real TCP sockets on localhost, and verifies:

1. A leader is elected with no manual intervention.
2. Writes through any node succeed and are readable afterward.
3. Killing the leader triggers a new election among the survivors.
4. The cluster keeps serving reads/writes after failover, and no
   previously-committed data is lost.
5. Deletes replicate correctly too.

I also drove it manually as three real OS subprocesses connected via the
actual CLI (not just in-process threads), confirming: normal put/get,
survival of a leader kill, and — important for demonstrating *correct*
behavior, not just optimistic-path behavior — that killing a **second**
node (dropping the cluster below a quorum) causes writes to fail cleanly
with a clear error rather than hanging or returning wrong data.

## Known limitations / natural next steps

This covers the heart of Raft, but intentionally leaves out a few things a
production system would need — good next milestones if you want to extend it:

- **No persistence.** All state is in memory; a node that crashes and
  restarts rejoins with a blank log rather than recovering its old one.
  Real Raft persists `currentTerm`, `votedFor`, and the log to disk before
  responding to RPCs.
- **No log compaction / snapshotting.** The log grows forever.
- **No linearizable reads.** `Get` is served from the leader's local state
  without a read-index or lease check, so a stale/partitioned leader could
  theoretically serve a stale read. Writes are still fully safe.
- **Static cluster membership.** The set of nodes is fixed in
  `cluster.json`; Raft's joint-consensus protocol for adding/removing nodes
  isn't implemented.

## Project layout

```
raftkv/
├── raft/
│   ├── rpc.py            # length-prefixed JSON-over-TCP transport
│   ├── node.py           # Raft: elections + log replication
│   ├── state_machine.py  # the KV store itself
│   └── server.py         # glue: RaftNode + state machine + RPCServer
├── kvserver.py            # entry point: run one cluster node
├── kvctl.py                # CLI client
├── cluster.json            # example 3-node cluster config
├── run_demo.sh / stop_demo.sh
└── tests/
    └── test_cluster.py    # real end-to-end cluster test
```
