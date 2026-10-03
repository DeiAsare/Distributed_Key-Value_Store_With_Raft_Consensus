#!/usr/bin/env bash
# Launches a 3-node cluster in the background (logs go to node<N>.log) and
# leaves it running. Use ./stop_demo.sh to shut it down.
set -e
cd "$(dirname "$0")"

for id in node1 node2 node3; do
  python3 kvserver.py --id "$id" --config cluster.json > "$id.log" 2>&1 &
  echo $! > "$id.pid"
done

echo "Cluster started. Tail logs with: tail -f node*.log"
echo "Try:  python3 kvctl.py --config cluster.json put name alice"
echo "      python3 kvctl.py --config cluster.json get name"
echo "Stop with: ./stop_demo.sh"
