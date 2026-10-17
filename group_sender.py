"""Send one status request to a group of Feed nodes. Python 3.10+.

Place this file beside your existing feed_server.py. No extra packages are
needed by this sender; Feed itself needs Flask and requests.

Run these in three separate terminals in the project directory:
    python feed_server.py
    python feed_server.py --port 5006 --group-port 6006 --node-name feed-2
    python group_sender.py --message-id group-demo-1

The default members are feed at 127.0.0.1:6003 and feed-2 at 127.0.0.1:6006.
To choose different addresses, supply --member NAME=HOST:PORT once per node.
Use the TCP group ports, not the HTTP ports 5003/5006.

One JSON message, including its ID, is reused for every member. Each TCP
connection carries one newline-terminated request and one acknowledgement.
Logs are written to logs/group_sender.log and the terminal. Exit code 0 means
every configured member acknowledged; 1 means at least one failed; 2 means
invalid command-line options. This is a status demo, without retries, durable
delivery, automatic membership discovery, or ordering across nodes.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import math
from pathlib import Path
import socket
import time
import uuid


MAX_MESSAGE_BYTES = 65536  # Includes the terminating newline, matching Feed.
DEFAULT_MEMBERS = ["feed=127.0.0.1:6003", "feed-2=127.0.0.1:6006"]


def parse_member(value):
    """Turn feed=127.0.0.1:6003 into (name, host, port)."""
    try:
        name, address = value.split("=", 1)
        host, port_text = address.rsplit(":", 1)
        name, host = name.strip(), host.strip().removeprefix("[").removesuffix("]")
        port = int(port_text)
        if not name or not host or not 1 <= port <= 65535:
            raise ValueError()
        return name, host, port
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "Use NAME=HOST:PORT, for example feed=127.0.0.1:6003"
        ) from error


def make_logger(log_dir):
    """Write UTC timestamps to both the console and a reusable evidence file."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("group_sender")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    formatter = logging.Formatter(
        "%(asctime)sZ [group_sender] %(message)s", "%Y-%m-%dT%H:%M:%S"
    )
    formatter.converter = time.gmtime
    for handler in (logging.StreamHandler(),
                    logging.FileHandler(Path(log_dir) / "group_sender.log", encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def marshal(message, logger):
    """Show the Python dictionary and the exact bytes sent over TCP."""
    logger.info("BEFORE_MARSHAL %r", message)
    wire = json.dumps(message, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    logger.info("AFTER_MARSHAL %r", wire)
    return wire


def read_acknowledgement(connection, deadline):
    """TCP may split a reply into several pieces; read until its newline."""
    received = bytearray()
    while b"\n" not in received and len(received) <= MAX_MESSAGE_BYTES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("deadline expired while waiting for acknowledgement")
        connection.settimeout(remaining)
        chunk = connection.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(received)))
        if not chunk:
            break
        received.extend(chunk)
    line, newline, _ = bytes(received).partition(b"\n")
    if not newline or len(line) + 1 > MAX_MESSAGE_BYTES:
        raise ValueError("acknowledgement must end with a newline and fit in 65536 bytes")
    return line + newline


def send_to_member(member, wire, message_id, timeout, logger):
    """Contact one member and return its result, even if that member fails."""
    name, host, port = member
    result = {"node": name, "address": f"{host}:{port}", "acknowledged": False}
    logger.info("GROUP_SEND group=service_nodes message_id=%s node=%s address=%s:%s",
                message_id, name, host, port)
    try:
        with socket.create_connection((host, port), timeout=timeout) as connection:
            # After connecting, bound the combined send/read phase. DNS lookup
            # and connection setup are separate from this deadline.
            deadline = time.monotonic() + timeout
            connection.settimeout(timeout)
            connection.sendall(wire)
            reply_wire = read_acknowledgement(connection, deadline)

        logger.info("BEFORE_UNMARSHAL node=%s wire=%r", name, reply_wire)
        reply = json.loads(reply_wire.decode("utf-8"))
        logger.info("AFTER_UNMARSHAL node=%s message=%r", name, reply)
        if not isinstance(reply, dict):
            raise ValueError("acknowledgement must be a JSON object")
        result["reply"] = reply
        if reply.get("success") is not True:
            raise ValueError(f"node rejected the message: {reply.get('error', 'no success=true')}")
        if reply.get("message_id") != message_id:
            raise ValueError("acknowledgement message_id does not match the request")
        if reply.get("node") != name:
            raise ValueError("acknowledgement node does not match the configured member name")
        if reply.get("status") != "ready":
            raise ValueError("acknowledgement status must be ready")
        result["acknowledged"] = True
        logger.info("GROUP_ACK message_id=%s node=%s status=ready", message_id, name)
    except (OSError, ValueError) as error:
        result["error"] = f"{type(error).__name__}: {error}"
        logger.error("GROUP_FAILED message_id=%s node=%s error=%s", message_id, name, result["error"])
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description="Send a status request to multiple Feed nodes")
    parser.add_argument("--member", action="append", type=parse_member,
                        help="NAME=HOST:PORT; repeat for each node (replaces the default members)")
    parser.add_argument("--message-id", help="Shared ID for this group request; defaults to a new UUID")
    parser.add_argument("--timeout", type=float, default=3.0,
                        help="connection timeout and post-connect exchange deadline in seconds")
    parser.add_argument("--log-dir", default="logs")
    args = parser.parse_args(argv)

    members = args.member or [parse_member(item) for item in DEFAULT_MEMBERS]
    if len(members) < 2:
        parser.error("configure at least two group members")
    if len({name for name, _, _ in members}) != len(members):
        parser.error("each member must have a different node name")
    if len({(host.lower(), port) for _, host, port in members}) != len(members):
        parser.error("each member must have a different host/port pair")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive finite number")
    if args.message_id is not None and not args.message_id.strip():
        parser.error("--message-id must not be empty")

    message_id = args.message_id if args.message_id is not None else str(uuid.uuid4())
    message = {"type": "demo_status_request", "group": "service_nodes", "message_id": message_id}
    logger = make_logger(args.log_dir)
    wire = marshal(message, logger)  # Marshal ONCE, then reuse the same bytes for all nodes.
    if len(wire) > MAX_MESSAGE_BYTES:
        parser.error("the marshalled group message is larger than 65536 bytes")
    logger.info("GROUP_START message_id=%s members=%s", message_id, members)

    # Each node gets its own connection. A failure does not stop other deliveries.
    with ThreadPoolExecutor(max_workers=min(8, len(members))) as workers:
        results = list(workers.map(
            lambda member: send_to_member(member, wire, message_id, args.timeout, logger), members))

    acknowledged = sum(result["acknowledged"] for result in results)
    logger.info("GROUP_RESULTS %s", json.dumps(results, ensure_ascii=False))
    logger.info("GROUP_SUMMARY message_id=%s members=%d acknowledged=%d failed=%d",
                message_id, len(members), acknowledged, len(members) - acknowledged)
    return 0 if acknowledged == len(members) else 1


if __name__ == "__main__":
    raise SystemExit(main())
