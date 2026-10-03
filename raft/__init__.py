"""A small, from-scratch Raft consensus implementation used to replicate a
key-value store across multiple nodes.

Modules:
    rpc            - length-prefixed JSON-over-TCP transport (our own wire protocol)
    node           - the RaftNode state machine (elections + log replication)
    state_machine  - the KV store that Raft-committed commands are applied to
    server         - glue that wires a RaftNode + state machine + RPCServer together
"""
