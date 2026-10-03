#!/usr/bin/env python3
"""Run one node of the cluster.

Usage:
    python3 kvserver.py --id node1 --config cluster.json
"""

import argparse
import json
import signal
import sys
import time

from raft.server import KVServer


def main():
    parser = argparse.ArgumentParser(description="Run one Raft KV store node")
    parser.add_argument("--id", required=True, help="this node's id, must be a key in the config file")
    parser.add_argument("--config", required=True, help="path to the cluster config JSON file")
    args = parser.parse_args()

    with open(args.config) as f:
        cluster = json.load(f)

    if args.id not in cluster:
        sys.exit(f"'{args.id}' is not in {args.config}. Known ids: {list(cluster)}")

    me = cluster[args.id]
    peers = {nid: (info["host"], info["port"]) for nid, info in cluster.items() if nid != args.id}

    server = KVServer(args.id, me["host"], me["port"], peers)
    server.start()
    print(f"[{args.id}] listening on {me['host']}:{me['port']}, peers={list(peers)}")

    def handle_sigint(_sig, _frame):
        print(f"\n[{args.id}] shutting down")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
