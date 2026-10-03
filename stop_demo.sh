#!/usr/bin/env bash
cd "$(dirname "$0")"
for id in node1 node2 node3; do
  if [ -f "$id.pid" ]; then
    kill "$(cat "$id.pid")" 2>/dev/null || true
    rm -f "$id.pid"
  fi
done
echo "Cluster stopped."
