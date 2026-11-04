# Milestone 3: P2P Integration

This milestone adds a peer-to-peer storage layer to our distributed social
network. Post records are no longer kept in Post's memory. They are stored on a
**Chord-style DHT**: a 32-position ring of independent peer processes, each with
its own SQLite database. Post finds the peer that owns a record by sending a
lookup that is routed through the ring, then reads or writes the record on that
peer. Feed, Notification, Broker, and Profile work as they did in Milestone 2.

The system demonstrates:

- Four or more peer nodes in a Chord ring (peers 4, 12, 20, 28)
- A peer joining (peer 18) and a peer leaving gracefully
- Lookups routed through the overlay using successor pointers and finger tables
- Resource discovery: posts are stored on, and found through, the peers
- Replication: each record has a primary on its owner and a backup on the next peer
- Failure handling: when a peer crashes, its successor promotes the backups and takes over

## 1. Required software

| Software | Purpose |
|---|---|
| Python 3.10 or newer | Runs every peer, service, client, and test. |
| pip | Installs Python dependencies. |
| Flask | Provides the HTTP endpoints of peers and services. |
| requests | Sends HTTP requests between peers and services. |
| curl | Sends lookup, state, and leave requests by hand (built into Windows 10/11 as `curl.exe`). |

SQLite comes with Python, so no database server is needed. Each peer creates
its own file, `peer_data/peer_<id>.db`.

## 2. Install dependencies

```bash
python --version
python -m pip install flask requests
python -c "import flask, requests; print('Dependencies are installed')"
```

If you use a virtual environment (for example PyCharm's `.venv`), install into
that environment. Run the commands in PyCharm's **Terminal** tab, where the prompt
starts with `(.venv)`, or call that environment's Python directly:

```bash
.venv\Scripts\python.exe -m pip install flask requests
```

## 3. Project files for this milestone

| File | Role |
|---|---|
| `chord_peer.py` | One Chord peer: routing, join, leave, replication, crash recovery |
| `peer_storage.py` | One peer's local SQLite storage (versions, tombstones, roles) |
| `chord_utils.py` | Ring math: intervals, finger starts, hashing post IDs to keys |
| `chord_client.py` | Used by Post: lookup through an entry peer, then get/put/delete on the owner |
| `post_server.py` | Post service; stores all post records on the peers |
| `demo_p2p.py` | Automated demo that starts real processes and checks every requirement |
| `test_chord_utils.py`, `test_peer_storage.py` | Local unit tests (no network) |
| `test_p2p_integration.py` | Integration tests that start real peers and services |
| `chord_contract.md` | Shared peer API and record format agreed by the team |

## 4. Start each service

Start every peer and service in its own terminal, from the project folder. In
VS Code use **Terminal > New Terminal**; in PyCharm click **+** in the Terminal
tab. Start them in the order shown, waiting for each peer to log `JOIN_COMPLETE`
(or `RING_CREATED` for the first one) before starting the next.

### Peers (Terminals 1–4)

```bash
python chord_peer.py --id 4  --port 7004
python chord_peer.py --id 12 --port 7012 --bootstrap http://127.0.0.1:7004
python chord_peer.py --id 20 --port 7020 --bootstrap http://127.0.0.1:7004
python chord_peer.py --id 28 --port 7028 --bootstrap http://127.0.0.1:7004
```

Peer 4 creates the ring. The other peers join through peer 4, the bootstrap
peer. Each peer logs to its terminal and to `logs/peer_<id>.log`.

### Services (Terminals 5–9)

```bash
python broker_server.py
python profile_server.py
python post_server.py
python feed_server.py --broker-url http://127.0.0.1:5005
python notification_server.py --broker-url http://127.0.0.1:5005
```

Start Broker before Feed and Notification so they can subscribe. Post uses peer 4
as its entry peer by default. To use a different peer, set `CHORD_ENTRY_PEER`
(for example `set CHORD_ENTRY_PEER=http://127.0.0.1:7012` in Command Prompt)
before starting Post.

| Process | Port |
|---|---|
| Peers 4, 12, 20, 28 (and joiner 18) | 7004, 7012, 7020, 7028 (7018) |
| Profile, Post, Feed, Notification, Broker | 5001, 5002, 5003, 5004, 5005 |

Post accepts writes only after the ring has at least two peers, because every
write needs a backup on another peer. Until then it answers HTTP 503.

**Terminal 10** is for client, `curl`, and test commands. The `curl` commands
below use Command Prompt syntax. In PowerShell, type `curl.exe` instead of `curl`.

## 5. Run the demo

### Option A: automated demo (all requirements, about one minute)

Stop any peers and services you started by hand first, because the demo uses the
same ports. Then run:

```bash
python demo_p2p.py
```

The demo starts its own peers, Profile, Post, and Feed in a fresh folder under
`p2p_runs/`. It runs every scenario, prints the log lines for each step, and
then stops everything it started. A successful run ends with:

```text
passed=28 failed=0 unrun=0
```

You can run the scenarios separately:

```bash
python demo_p2p.py membership   # 4 peers, lookup, store/fetch, peer 18 join and leave
python demo_p2p.py crash        # peer 20 is killed and peer 28 recovers its data
```

Each run's logs and databases stay in `p2p_runs/<timestamp>-<scenario>/` for screenshots.

### Option B: step-by-step demo (for the video)

With Terminals 1–9 running, run each step in Terminal 10.

**1. Peers started.** Show each peer's neighbours and finger table:

```bash
curl http://127.0.0.1:7004/state
curl http://127.0.0.1:7012/state
curl http://127.0.0.1:7020/state
curl http://127.0.0.1:7028/state
```

Expected ring: 4 → 12 → 20 → 28 → 4. For example, peer 4 has
`predecessor` 28, `successor` 12, and fingers `[12, 12, 12, 12, 20]`.

**2. Store resources through the peers.** Run the client demo and create posts
for the starter users. Profile's starter data says user 2 follows users 7 and 9.

```bash
python client.py demo
python client.py post 7 "Milestone 3 post by user 7" pic.jpg
python client.py post 9 "Milestone 3 post by user 9" pic2.jpg
```

`client.py demo` should end with `DEMO PASSED`. Post saves each post on the peer
that owns its key and on that peer's successor as backup. Check the `records`
counts in `/state` to see where the copies went.

**3. Lookup routed through the overlay.**

```bash
curl -X POST http://127.0.0.1:7004/lookup -H "Content-Type: application/json" -H "X-Request-ID: video-lookup-17" -d "{\"key\":17}"
curl -X POST http://127.0.0.1:7004/lookup -H "Content-Type: application/json" -H "X-Request-ID: video-lookup-25" -d "{\"key\":25}"
```

| Key | Expected path | Owner | Why |
|---|---|---|---|
| 17 | `[4, 12]` | 20 | Peer 4 forwards to 12; 17 falls in (12, 20], so 12's successor owns it |
| 25 | `[4, 20]` | 28 | Peer 4 uses its finger to 20, skipping 12 (finger-table shortcut) |

To see every hop of one lookup across the peer logs:

```bash
findstr "video-lookup-25" logs\peer_*.log
```

(On macOS/Linux: `grep video-lookup-25 logs/peer_*.log`.) The lines show
`RECV`, `LOOKUP decision=forward-finger next=20`, `SEND dest=20`, and peer 20's
final `LOOKUP decision=successor-owns owner=28`.

**4. Resource discovery.** Feed asks Profile who user 2 follows, then asks Post
for those authors' posts. Post finds each post through a Chord lookup.

```bash
python client.py feed 2
```

The reply has `success: true` and the posts by users 7 and 9, newest first.

**5. A peer joins.** In a new terminal:

```bash
python chord_peer.py --id 18 --port 7018 --bootstrap http://127.0.0.1:7004
```

Peer 18 joins between 12 and 20 and takes over keys (12, 18] from peer 20.
Look for these log lines: `TRANSFER source=20 dest=18 ... role=primary->primary`,
`SUCCESSOR_SET`, `PREDECESSOR_SET`, and `JOIN_COMPLETE predecessor=12 successor=20`.
Then check the change:

```bash
curl -X POST http://127.0.0.1:7004/lookup -H "Content-Type: application/json" -d "{\"key\":17}"
python client.py feed 2
```

Key 17 now resolves to peer **18**, and the feed still returns every post.

**6. A peer leaves.**

```bash
curl -X POST http://127.0.0.1:7018/leave -H "X-Request-ID: video-leave-1"
```

The reply is `{"status": "left", ...}`, and the peer 18 process exits. It hands
its data to peer 20 (primary) and peer 28 (backup) before it unlinks. Look for
`TRANSFER` and `LEAVE_COMPLETE handed_to=20` in `logs/peer_18.log`. Key 17
resolves to peer 20 again, and `python client.py feed 2` still returns every post.

**7. Failure when a peer disappears.** Press **Ctrl+C** in peer 20's terminal, or
close that terminal. Peer 20 stops without handing anything off. Within about
10 seconds, peer 28's log shows:

```text
PREDECESSOR_UNREACHABLE peer=20 failures=1
PREDECESSOR_UNREACHABLE peer=20 failures=2
PREDECESSOR_UNREACHABLE peer=20 failures=3
RECOVERY_START dead=20
TRANSFER ... role=primary->backup
RECOVERY_COMPLETE dead=20 new_predecessor=12
```

Peer 28 promotes the backups it held for peer 20 and copies them to peer 4 as new
backups. Peer 12 re-sends its own backups to peer 28. The ring is now
4 → 12 → 28, key 17 resolves to peer 28, and `python client.py feed 2` still
returns every post.

### Starting over

Peers keep their data in `peer_data/` between runs. To start with an empty ring,
stop every peer and delete the `peer_data` folder. The automated demo and the
tests always use their own fresh folders under `p2p_runs/`.

## 6. Reproduce the test cases

Stop any peers and services you started by hand first, because the integration
tests use the same ports.

```bash
python -m unittest test_chord_utils test_peer_storage test_p2p_integration -v
```

Expected result: `Ran 29 tests ... OK`.

| Test file | Tests | What it checks |
|---|---|---|
| `test_chord_utils.py` | 5 | Ring intervals with wraparound, finger starts, closest preceding finger, post-ID hashing |
| `test_peer_storage.py` | 8 | Per-peer SQLite files, versioning, duplicate operations, tombstones, transfer intervals, role changes |
| `test_p2p_integration.py` | 16 | Real peers: ring and finger tables, routed lookups with log trace, primary/backup placement, join, leave, crash recovery, duplicate-ID join rejected, Post and Feed over the peers, concurrent posts/likes, deletes not restored by join or leave |

To run only the fast local tests (under one second, nothing started):

```bash
python -m unittest test_chord_utils test_peer_storage -v
```

`python demo_p2p.py` is also a repeatable test. It exits with code 0 only when no
check fails, and writes a `summary-<timestamp>.json` file to `p2p_runs/`.

## 7. Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: No module named 'flask'` | Install into the Python that runs the project: `python -m pip install flask requests` (see Section 2). |
| `ports already in use` | A peer or service from an earlier run is still running. Press Ctrl+C in its terminal, or end the leftover Python processes in Task Manager. |
| Windows Firewall prompt | Click **Allow**; peers must accept local connections. |
| Post returns 503 `writes paused` | A join, leave, or recovery is in progress. Wait a few seconds and retry. |
| Post returns 503 `no distinct successor` | Only one peer is running. Start at least two peers. |

## 8. Known limits

- Only one membership change (join or leave) can run at a time. The ring pauses
  writes during it.
- Exactly one Post process is assumed. Post serializes its own writes, so two
  Post processes could hand out the same post ID.
- Each record has one backup. The ring survives one crash at a time, but not two
  neighbouring peers crashing together.
- Deleted records leave tombstones that are never purged.
