#!/usr/bin/env python3
"""Command-line client for the Raft KV store.

Usage:
    python3 kvctl.py --config cluster.json put name alice
    python3 kvctl.py --config cluster.json get name
    python3 kvctl.py --config cluster.json delete name

The client doesn't need to know who the leader is: it starts at whichever
node it's told about, and if that node isn't the leader, follows the
leader_hint the node sends back (or round-robins if no hint is available).
"""

import argparse
import json
import sys
import time

from raft import rpc


def send_with_retry(cluster, message, tries=15, retry_delay=0.2):
    node_ids = list(cluster.keys())
    target = node_ids[0]

    for _attempt in range(tries):
        host, port = cluster[target]["host"], cluster[target]["port"]
        resp = rpc.call(host, port, message, timeout=0.5)

        if resp is None:
            target = node_ids[(node_ids.index(target) + 1) % len(node_ids)]
            time.sleep(retry_delay)
            continue

        if resp.get("success"):
            return resp

        hint = resp.get("leader_hint")
        if hint and hint in cluster:
            target = hint
        else:
            target = node_ids[(node_ids.index(target) + 1) % len(node_ids)]
        time.sleep(retry_delay)

    return {"success": False, "error": "no leader found after retries"}


def main():
    parser = argparse.ArgumentParser(description="Talk to the Raft KV store cluster")
    parser.add_argument("--config", required=True, help="path to the cluster config JSON file")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get")
    p_get.add_argument("key")

    p_put = sub.add_parser("put")
    p_put.add_argument("key")
    p_put.add_argument("value")

    p_del = sub.add_parser("delete")
    p_del.add_argument("key")

    args = parser.parse_args()
    with open(args.config) as f:
        cluster = json.load(f)

    if args.cmd == "get":
        resp = send_with_retry(cluster, {"type": "ClientGet", "key": args.key})
    elif args.cmd == "put":
        resp = send_with_retry(cluster, {"type": "ClientPut", "key": args.key, "value": args.value})
    else:
        resp = send_with_retry(cluster, {"type": "ClientDelete", "key": args.key})

    print(json.dumps(resp, indent=2))
    sys.exit(0 if resp.get("success") else 1)


if __name__ == "__main__":
    main()
