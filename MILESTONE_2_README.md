# Milestone 2: Communication

This milestone adds communication between the Profile, Post, Feed, Broker,
Notification, and Client programs in our distributed social network.
The system demonstrates HTTP request-reply, publish-subscribe notifications,
TCP group communication, JSON marshalling/unmarshalling, and timestamped logs.

## 1. Required software

| Software | Purpose |
|---|---|
| Python 3.10 or newer | Runs every service, the client, and the group sender. |
| pip | Installs Python dependencies. |
| Flask | Provides the HTTP endpoints. |
| requests | Sends HTTP requests between services. |

The client and group sender use only Python's standard library. Werkzeug is
installed as a Flask dependency. No external message-broker installation is
needed: `broker_server.py` is the project's broker.

For this milestone, the running services keep their data in memory. No separate
database setup is required for the demonstrations below.

## 2. Install dependencies

```bash
python --version
python -m pip install flask requests
python -c "import flask, requests, werkzeug; print('Dependencies are installed')"
```

## 3. Start each service

Each service runs continuously, so start it in its own terminal. In VS Code,
select **Terminal > New Terminal** for each one. Keep every service running
while using the client. Stop an old process with **Ctrl+C** before starting
another process on the same port.

**Terminal 1 — Broker:**

```bash
python broker_server.py
```

**Terminal 2 — Profile:**

```bash
python profile_server.py
```

**Terminal 3 — Post:**

```bash
python post_server.py
```

**Terminal 4 — Feed:**

```bash
python feed_server.py --post-url http://127.0.0.1:5002 --broker-url http://127.0.0.1:5005
```

**Terminal 5 — Notification:**

```bash
python notification_server.py --broker-url http://127.0.0.1:5005
```

Start Broker before Feed and Notification. Wait for Feed to log its subscription
to `post.created`. Notification should log subscriptions to `post.created`,
`post.liked`, and `post.commented`.

**Terminal 6 — Client and group-sender commands:**

Leave this terminal available for the commands in Sections 4 and 5. The client
and group sender exit after completing a command.

**Terminal 7 — Second Feed node:**

Start this node when preparing the group demo:

```bash
python feed_server.py --port 5006 --group-port 6006 --node-name feed-2
```

The second node only needs its TCP receiver for this demonstration, so this
command does not subscribe it to the broker. The two pub-sub subscribers remain
the first Feed process and Notification.

## 4. Run the demo

### Complete client demo

With Terminals 1–5 running, execute this in Terminal 6:

```bash
python client.py demo
```

The demo performs these steps:

1. Creates or reuses Alice's profile (user ID **10**) and Bob's profile (user ID **11**).
2. Makes Bob follow Alice.
3. Creates a post by Alice. Post publishes `post.created` to Broker, which sends
   it to Feed and Notification. The client checks delivery to at least two
   subscribers and checks Bob's new-post notification.
4. Has Bob like and comment on Alice's post. These events produce notifications
   for Alice.
5. Requests Bob's feed and verifies that Alice's new post appears.
6. Requests Alice's notifications and verifies the like and comment entries.

A successful run ends with a line similar to:

```text
DEMO PASSED author_id=10 follower_id=11 post_id=1
```

The actual post ID depends on previous activity. The client uses the returned
ID instead of assuming it is always 1. A failed step produces `DEMO FAILED`
and an unsuccessful exit code.

Running the demo again reuses matching profiles and creates another post with
new likes, comments, and notifications. It does not clear previous data.
If IDs 10 or 11 already belong to different usernames, use unused IDs:

```bash
python client.py demo --author-id 20 --follower-id 21
```

The remaining examples assume the default IDs 10 and 11. Adjust them if you
selected different IDs. Keep the services running after the demo so the data
remains available.

### Show request-reply communication

```bash
python client.py feed 11
```

Show the client's `SEND` line for `GET /feed/11`, the `RECV` line with HTTP
status 200, and the returned JSON. The reply should contain `success: true`,
`user_id: 11`, and Alice's posts. Feed requests Bob's following list from
Profile, retrieves the relevant posts from Post, and sorts them newest first.

### Show publish-subscribe notifications

Create another post:

```bash
python client.py post 10 "Milestone 2 pub-sub test" images/demo.jpg
```

The image path is sent as text metadata; no image file or upload is needed.
The reply should include `event_published: true` when delivery succeeds.

1. Open `logs/broker.log` and find the new event's `PUBLISH` and `PUBLISH_DONE`
   entries. The topic should be `post.created`; with the startup configuration
   above, the result should be `matched=2 delivered=2 failed=0`.
2. Copy its `event_id` and find an `EVENT_APPLIED` entry with that ID in both
   `logs/feed.log` and `logs/notification.log`.
3. Request Bob's notifications:

   ```bash
   python client.py notifications 11
   ```

4. Show the new notification with `type: "new_post"`, `from_user_id: 10`,
   and the newly created post's ID.

Matching event IDs in both subscriber logs demonstrate that both services
received the same publication. Feed records received events; displayed feeds
are still assembled from fresh Post requests.

### Show group communication

With both Feed nodes running, execute:

```bash
python group_sender.py --message-id milestone2-group-1
```

Expected output includes the following; acknowledgement order can differ:

```text
GROUP_ACK message_id=milestone2-group-1 node=feed status=ready
GROUP_ACK message_id=milestone2-group-1 node=feed-2 status=ready
GROUP_SUMMARY message_id=milestone2-group-1 members=2 acknowledged=2 failed=0
```

Find `GROUP_RECEIVED` with that message ID in both `logs/feed.log` and
`logs/feed-2.log`. These are two independent Feed processes on ports 6003 and
6006. The sender sends the same message over a separate TCP connection to each
node and validates each acknowledgement's node name, message ID, and status.

### Show the message before and after marshalling

Open `logs/group_sender.log` after the group test. The relevant entries have
the following form, with timestamps omitted:

```text
BEFORE_MARSHAL {'type': 'demo_status_request', 'group': 'service_nodes', 'message_id': 'milestone2-group-1'}
AFTER_MARSHAL b'{"type": "demo_status_request", "group": "service_nodes", "message_id": "milestone2-group-1"}\n'
```

Before marshalling, the message is a Python dictionary. After marshalling, it
is JSON encoded as UTF-8 bytes. The `b'...'` notation is Python's display of
bytes; the newline marks the end of the TCP message.

In `logs/feed.log`, show the matching `GROUP BEFORE_UNMARSHAL` and
`GROUP AFTER_UNMARSHAL` entries. The receiver decodes the bytes and parses JSON
back into a Python dictionary. The message ID remains unchanged.