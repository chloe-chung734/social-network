# Milestone 4: Coordination

This milestone demonstrates vector clocks, leader election, distributed mutual exclusion, and coordinator failure recovery in our social-network application.

The demo uses ZooKeeper for coordination and protects shared post data from conflicting writes.

## 1. Required Software

- **Python 3.13**
- **ZooKeeper**, running at `127.0.0.1:2181`
- **Flask**, **requests**, **Kazoo**, and **pytest**
- **VS Code** or another editor for running Python and viewing logs

ZooKeeper can run through Docker or a direct installation. Docker Desktop is only required if using the included Docker Compose configuration. A direct ZooKeeper installation requires Java.

The commands below use Windows PowerShell and should be run from the project's root folder.

## 2. How to Install Dependencies

Open a terminal in the `social-network` project folder and run:

```powershell
py -3.13 -m pip install flask requests -r requirements-coordination.txt
```

Check that the demo's required modules are available:

```powershell
py -3.13 demo_coordination.py --check
```

This command checks dependencies. It does not start ZooKeeper or run the demonstration.

## 3. How to Start the Services

**Only ZooKeeper needs to be started separately.** The `demo_coordination.py` script starts all application services automatically.

If ZooKeeper is already running, continue to Section 4.

If using Docker, start Docker Desktop and run:

```powershell
docker compose -f compose.zookeeper.yml up -d
```

If using a direct ZooKeeper installation, start it using that installation's startup command and leave it running during the demo.

The demo automatically starts:

| Service | Purpose | Port |
|---|---|---|
| Three coordinator candidates | Elect a leader and manage lock requests | 5101–5103 |
| Four Chord peers | Store shared post data | 7004, 7012, 7020, 7028 |
| Profile | Provide user and following information | 5001 |
| Post A and Post B | Process competing post updates | 5002, 5006 |
| Feed | Retrieve posts and receive events | 5003, 6003 |
| Notification | Process notification events | 5004 |
| Broker | Distribute published events | 5005 |

Do not start these services manually before running the demo. Their ports must be available so the script can start its own processes.

## 4. How to Run the Demo

With ZooKeeper running, open `demo_coordination.py` in VS Code and select **Run Python File in Terminal**. Use the Python interpreter where the dependencies were installed.

Alternatively, run this command from the project folder:

```powershell
py -3.13 demo_coordination.py
```

The script automatically:

1. Starts three coordinator candidates and verifies that one leader is elected.
2. Starts the Chord peers and application services.
3. Creates a post and checks delivery to Feed and Notification.
4. Sends competing like requests through Post A and Post B.
5. Checks that writers wait for the lock and enter their critical sections exclusively.
6. Stops the active coordinator while a writer holds the lock and two requests are waiting.
7. Verifies that a replacement coordinator preserves the lock holder and waiting queue.
8. Completes the pending writes and performs another round of competing requests.
9. Checks vector timestamps, event ordering, and preservation of the expected updates.

The terminal may remain quiet while the services write their logs.

A successful run prints:

```text
PASS: evidence in <run-folder>
```

The script stops its application processes when finished. ZooKeeper remains running separately.

### Viewing the Demo Results

In VS Code Explorer, open `coordination_runs`, then open the run folder identified in the terminal.

| File | Evidence |
|---|---|
| `summary.json` | Overall result, completed checks, and message timestamps |
| `logs/coord-1.clock.jsonl` | Coordinator 1's election and lock events |
| `logs/coord-2.clock.jsonl` | Coordinator 2's election and lock events |
| `logs/coord-3.clock.jsonl` | Coordinator 3's election and lock events |
| `logs/post-a.clock.jsonl` | Post A's lock waiting and critical-section events |
| `logs/post-b.clock.jsonl` | Post B's lock waiting and critical-section events |
| `logs/holder-2.clock.jsonl` | The lock holder's activity during coordinator replacement |
| `client-3-0.result.json` and `client-3-1.result.json` | Post data returned by the final requests |

Use **Ctrl+F** to locate these events:

- `ELECTION_JOIN`: a coordinator registers as an election candidate.
- `LEADER_READY`: a coordinator becomes leader.
- `STATE_RECOVERED`: a coordinator recovers the stored coordination state.
- `LOCK_WAIT`: a writer waits for permission to access the shared resource.
- `CS_ENTER`: a writer enters its critical section.
- `CS_EXIT`: a writer finishes its critical section.

The `exchanges` section in `summary.json` contains `sent`, `reply`, and `received` vector timestamps.

Of the two final client results, the one representing the last completed update should contain likes from users **22, 23, 24, 25, 26, 27, and 90**. Their order may vary.

## 5. How to Reproduce the Test Cases

### Repeat the Demonstration

Keep ZooKeeper running and run the demo again:

```powershell
py -3.13 demo_coordination.py
```

Each run creates a separate results folder and its own ZooKeeper coordination namespace.

| Test case | Expected result |
|---|---|
| Leader election | Exactly one active coordinator is detected |
| Logical timestamps | Recorded messages contain vector-clock metadata |
| Concurrent requests | Independent clients produce concurrent request origins |
| Mutual exclusion | Tested critical sections are causally ordered without overlap |
| Conflicting updates | All expected likes are preserved |
| Coordinator failure | Another coordinator takes over |
| State recovery | The existing lock holder and waiting queue are preserved |
| Continued operation | Pending and subsequent requests complete after recovery |

This failure test covers a coordinator crash while ZooKeeper and the Chord peers remain available.

Review the terminal result and generated `summary.json` to confirm that the demonstration passed.