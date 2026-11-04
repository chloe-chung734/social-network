# Chord storage contract for Milestone 3

Status: proposed shared interface, based on the inspected social-network
repository and the five teammate task descriptions. The repository currently
has no Chord peer, storage adapter, or Chord API, so the peer endpoints below
are decisions for the team to confirm before modules are implemented against
them. Existing application endpoints below are already present in the project.

## Scope and ownership

- Four independent peer processes store post records in the 32-position ring.
  Initial planned peers: IDs 4, 12, 20, 28 on ports 7004, 7012, 7020, 7028.
  Peer 18 on port 7018 demonstrates join and leave.
- Profile, Post, Feed, Broker, and Notification stay outside the ring. Post is
  the only application service that accesses peer storage. Feed continues to
  call Post through its existing `/posts/retrieve` route.
- The peer owns routing, membership, transfer, and network backup replication.
  The storage adapter owns one peer's SQLite records and local record roles.
  The Chord client starts at one configured entry peer and does not hold a
  global peer list or calculate record ownership itself.
- Peer APIs use JSON over HTTP. Every request accepts/forwards `X-Request-ID`.
  Positive IDs at application boundaries are JSON integers (booleans and
  numeric strings are invalid).

## Routing and record identity

- Ring positions are integers 0 through 31. A peer owns keys in the clockwise
  interval `(predecessor_id, peer_id]`, including wrap around. For peers
  `[4,12,20,28]`, key 17 belongs to 20; after peer 18 joins, it belongs to 18.
- For a post ID `n`, calculate `SHA-256(ASCII(decimal(n)))`, interpret the
  digest as an unsigned big-endian integer, and take modulo 32. The full post
  ID remains the record identity; routing-key collisions must not overwrite.
- Chord lookup starts at an entry peer and routes through peer requests.
  Exact peer-ID keys resolve to that peer. A one-peer ring owns every key.
  Bound hops/requests and return an error if routing cannot make progress.
- Record IDs are strings: `post:<positive integer>`, `author:<positive
  integer>`, and `meta:next_post_id`. Post records use the post's hash key.
  For non-post metadata/index records, hash the UTF-8 record ID with SHA-256
  modulo 32. The record ID, not routing key alone, is the unique key.

## Proposed peer HTTP API

Base URL is a peer's reachable `http://host:port`. All success/error bodies
are JSON objects. Success responses include `{"success":true}`. Errors use
`{"success":false,"error":"human-readable reason"}`. Suggested statuses:
400 malformed request, 404 absent resource where relevant, 409 duplicate
peer/conflict, 503 unavailable/not ready/write paused, 504 operation timeout.
Peer failures must not be translated into a successful empty author feed.

### Health and state

- `GET /health` -> `{"success":true,"peer_id":12,"status":"ready"}`.
  Status is `starting`, `transferring`, `ready`, or `recovering`; only `ready`
  accepts ordinary application writes.
- `GET /state` -> `{"success":true,"peer_id":12,"predecessor":{"id":4,
  "url":"http://127.0.0.1:7004"},"successor":{"id":20,
  "url":"http://127.0.0.1:7020"},"fingers":[...],"ready":true}.

### Lookup

- `POST /lookup` with `{"key":17}` ->
  `{"success":true,"owner":{"id":20,"url":"http://127.0.0.1:7020"},
  "hops":2}`.
- The entry peer routes the request using its successor/fingers and returns
  the discovered owner. The caller then performs record operations against
  that owner. Include `X-Request-ID` in every hop's logs.

### Join and graceful leave

- Start first peer without bootstrap; it creates a one-peer ring and becomes
  ready. Start additional peers with an existing bootstrap peer. Proposed
  command shape: `python chord_peer.py --id ID --port PORT --db-dir DIR` and,
  for joining peers, `--bootstrap http://HOST:PORT`. Confirm final flags with
  the peer owner before using demo scripts.
- `POST /join` on the joining peer with `{"bootstrap_url":"..."}` returns
  only after neighbors are updated, records in `(predecessor,new_peer_id]`
  have been imported and confirmed, and the peer is ready. Duplicate live
  IDs are rejected. Peer 18 joining between 12 and 20 takes `(12,18]`.
- `POST /leave` on a peer requests graceful leave. It responds success only
  after records are accepted by their new owners, neighbors are reconnected,
  and the peer is safe to stop.
- Demo membership changes are serialized: one join/leave at a time. The peer
  owner pauses writes (503 with a retryable message) before transfer, confirms
  each destination write before deleting/changing source role, refreshes
  routing, and resumes writes only after all peers report ready. Post must
  surface the pause as storage unavailable; it must not say a write succeeded.

### Record operations

The peer is the network boundary for the storage adapter. Primary record
values are the complete JSON Post object, including `id`, `author`,
`timestamp`, `content`, `caption`, `likes`, and `comments`. Index and metadata
values are JSON objects described below.

- `POST /records/get` with `{"record_id":"post:17","routing_key":3}` ->
  found: `{"success":true,"found":true,"record":{"id":17,...}}`; absent:
  `{"success":true,"found":false}`. A missing record is not a peer error.
- `POST /records/put` with `{"record_id":"post:17","routing_key":3,
  "value":{"id":17,...},"operation_id":"stable-operation-id"}` ->
  `{"success":true,"stored":true,"role":"primary"}` only after the
  primary write and required backup replication are confirmed. The operation
  ID makes an identical retry safe. If a timeout makes commit uncertain, the
  client does not blindly retry under a new operation ID.
- `POST /records/delete` with `{"record_id":"post:17","routing_key":3,
  "operation_id":"stable-operation-id"}` -> success after the owner records
  a deletion/tombstone and replicates it, so a delayed backup cannot resurrect
  a deleted post. Repeating the same operation is safe.
- Storage adapter operations are local functions, not HTTP endpoints: get,
  put/version check, delete/tombstone, list records in a clockwise interval,
  import transferred record, change local role, and promote backup. Keep full
  record ID separate from key; reject stale versions and make transfers
  repeatable. Peer performs all remote replication and transfer calls.
- One backup is held by the owner's next reachable successor. A primary write
  counts as successful only when primary and backup are confirmed (or the team
  explicitly agrees on a degraded policy and reports that state separately).
  On one peer's failure in a multi-peer ring, recover from a reachable successor's backup, promote it,
  restore its backup placement, then report recovery complete. Crash recovery
  is distinct from graceful leave.
- A peer may accept post mutations only when it is ready and a distinct successor can confirm the   
  backup; otherwise, return HTTP 503 immediately. Lookups and reads may continue, and the request must not wait for another peer to join.

## Post-side records and consistency

- `post:<id>` value is the complete public post object. Do not use a cached
  full-post collection in Post as the source of truth.
- `author:<id>` value is `{"post_ids":[1,2,...]}`. Post gets the index through
  Chord and fetches every referenced post record through Chord. An absent
  index means no posts and can return an empty successful list. If any
  referenced post cannot be fetched, Post returns an error instead of a
  partial list.
- `meta:next_post_id` value is `{"value":N}` and preserves IDs across Post
  restarts when peer data persists. The class demo assumes one Post process;
  concurrent Post instances require a peer-side atomic counter allocation.
- Post/index writes are separate peer operations; they are not a transaction.
  On index failure after a new post write, Post reports failure and attempts
  compensation, while documenting that the result can be uncertain. A
  successful operation ID is stable across retries. Do not generate a new ID
  and retry after a timeout that may have committed.
- Author retrieval uses one shared total budget below Feed's fixed 10-second
  timeout (recommend 7 seconds including index and all post gets). Map storage
  timeout to Post HTTP 504 and unavailable/not-ready storage to HTTP 503.

## Existing application APIs that remain in use

These routes were found in the current repository; Part 4 should preserve
them. They are not Chord peer endpoints.

### Post service (current default port 5002)

- `GET /health` -> `{"service":"post","status":"ok"}`.
- `POST /posts` accepts `{"author":7,"content":"...","caption":"..."}`
  (also accepts aliases `user_id` and `image_url`); successful creation is
  HTTP 201 with `{"success":true,"id":N,"post":{...},
  "event_published":true|false}`.
- `POST /posts/retrieve` accepts `{"author":7}` and returns
  `{"success":true,"posts":[...]}`. Feed depends on this exact shape.
- Preserve `DELETE /posts/<post_id>`, `POST /posts/<post_id>/comments`,
  `DELETE /posts/<post_id>/comments/<comment_id>`, both like aliases
  `POST /posts/<post_id>/likes` and `/like`, and
  `DELETE /posts/<post_id>/likes`. Preserve their existing success/error
  response shapes and publish events after successful stored changes.
- Post forwards `X-Request-ID` and returns it on responses. Storage failures
  use 503/504; broker failure is reported separately from storage success.

### Feed service (current default port 5003)

- `GET /feed/<user_id>` obtains followee IDs from Profile, then for each ID
  calls `POST <POST_URL>/posts/retrieve` with `{"author":ID}`. It expects
  `success:true`, a `posts` array, each post's author matching the requested
  author, and a parseable timezone-aware timestamp. It sorts newest first.
- Feed's Post request timeout is 10 seconds. Do not change Feed for this task.

### Profile service (current default port 5001)

- `GET /profiles/<user_id>/following` ->
  `{"user_id":ID,"following":[AUTHOR_ID,...]}`; Feed validates this shape.
- `GET /profiles/<user_id>/followers` ->
  `{"user_id":ID,"followers":[USER_ID,...]}`; Notification uses it.
- Other existing profile routes include create/read/update, add follow, and
  remove follow: `POST /profiles`, `GET /profiles/<id>`,
  `PUT|PATCH /profiles/<id>`, `POST /profiles/<id>/following`, and
  `DELETE /profiles/<id>/following/<target_id>`.

### Broker and event compatibility (current default port 5005)

- `POST /publish` accepts an event object with a nonempty `event_id` and
  `type`. `post.created` must include `post` with positive integer `id` and
  `author`, and string `timestamp`, `content`, and `caption`. The broker
  responds with delivery fields including `success`, `event_id`, `topic`,
  `matched`, `delivered`, `failed`, `all_delivered`, and `results`.
- Keep existing event types/payloads: `post.created` includes the full post;
  `post.commented` and `post.liked` include `post_id`, `post_author`, and
  `user_id`. Publish only after the corresponding authoritative Chord write
  succeeds. A broker delivery failure does not undo stored data.
- Existing subscription endpoints are `POST /subscribe` and
  `POST /unsubscribe`, both using `{"topic":"...","url":"..."}`.
  Notification and Feed subscribe to `post.created` when configured.

## Manual integration acceptance checks

1. Start four separate peers with separate SQLite paths; wait for each
   `/health` to say ready. Start Post with its entry peer URL configured.
2. Create a Post through `POST /posts`; inspect the responsible peer database
   to confirm the full record and successor backup. Retrieve it via
   `POST /posts/retrieve` and via unchanged Feed.
3. Restart only Post and confirm the same post ID and content remain
   retrievable. Exercise delete, like/unlike, and comment create/delete.
4. Start peer 18, confirm key 17 owner changes from 20 to 18, and verify
   transferred data is still fetchable. Gracefully leave 18 and verify return
   of ownership and data. Do not treat peer 18's graceful leave as a crash test.
5. Separately test a peer crash only if implementing the report's backup
   recovery claim. Confirm backup promotion and restoration, not only a 200.
6. Stop peers or pause writes and verify Post returns 503/504 as applicable;
   verify Feed does not receive a false empty successful list.

## Agreement checklist

Before implementation, the peer, storage, and Post owners should confirm the
endpoint paths, owner URL shape, record ID/key/value format, write and backup
success rule, operation ID/version behavior, join/leave readiness sequence,
and startup flags. Update this document once those implementations are merged
so it describes the actual interface rather than this proposal.
