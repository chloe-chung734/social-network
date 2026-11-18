"""Thread-safe vector clocks shared by every process in the milestone.

A vector is a JSON object mapping a process identity to a counter:

    {"post-a:<boot-id>": 3, "coord-1:<boot-id>": 5}

Rules (implemented here, nowhere else):
    tick()           local application event: increment own component
    send()           increment own component, return an immutable snapshot
    receive(remote)  component-wise maximum, then increment own component
    snapshot()       copy of the current vector; does NOT advance the clock

Missing components mean zero. Learned components are never removed.
Every returned vector is a fresh dict, so callers cannot change the clock
by editing a result. Malformed vectors raise VectorClockError and leave the
clock unchanged.

compare_vectors(a, b) returns "before", "after", "equal" or "concurrent".
Never sort vectors lexicographically and call that causal ordering.
"""

import json
import threading

MAX_COUNTER = 2 ** 63 - 1
MAX_COMPONENTS = 1024          # distinct processes in one vector
MAX_ID_LENGTH = 300            # characters in one component name
MAX_ENCODED_LENGTH = 65536     # characters accepted by decode_vector


class VectorClockError(ValueError):
    """A vector, counter or identity is malformed, or a counter overflowed."""


def _check_identity(node_id):
    if not isinstance(node_id, str) or not node_id or len(node_id) > MAX_ID_LENGTH:
        raise VectorClockError(
            "node identity must be a nonempty string of at most %d characters"
            % MAX_ID_LENGTH)
    return node_id


def validate_vector(vector):
    """Return a validated COPY of a vector or raise VectorClockError.

    Keys must be nonempty strings; values must be integers 0..MAX_COUNTER.
    Booleans, floats, strings and negative numbers are rejected.
    """
    if not isinstance(vector, dict):
        raise VectorClockError("vector must be a JSON object (dict)")
    if len(vector) > MAX_COMPONENTS:
        raise VectorClockError("vector has more than %d components" % MAX_COMPONENTS)
    checked = {}
    for key, value in vector.items():
        _check_identity(key)
        if type(value) is not int:
            raise VectorClockError("counter for %r must be an integer" % key)
        if value < 0 or value > MAX_COUNTER:
            raise VectorClockError("counter for %r is out of range" % key)
        checked[key] = value
    return checked


def encode_vector(vector):
    """Compact, deterministic JSON text for X-Vector-Clock headers."""
    return json.dumps(validate_vector(vector), sort_keys=True,
                      separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _no_duplicate_keys(pairs):
    keys = [key for key, _ in pairs]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate component name")
    return dict(pairs)


def _reject_constant(name):
    raise ValueError("%s is not valid JSON" % name)


def decode_vector(text):
    """Parse and validate compact JSON text (str or bytes) into a vector."""
    if isinstance(text, (bytes, bytearray)):
        try:
            text = bytes(text).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise VectorClockError("vector is not valid UTF-8") from exc
    if not isinstance(text, str):
        raise VectorClockError("encoded vector must be text")
    if len(text) > MAX_ENCODED_LENGTH:
        raise VectorClockError("encoded vector is too large")
    try:
        data = json.loads(text, object_pairs_hook=_no_duplicate_keys,
                          parse_constant=_reject_constant)
    except ValueError as exc:
        raise VectorClockError("vector is not valid JSON: %s" % exc) from exc
    return validate_vector(data)


def compare_vectors(a, b):
    """Causal relation of vector a to vector b.

    "before"      a happened-before b (every a <= b, at least one smaller)
    "after"       b happened-before a
    "equal"       identical after treating missing components as zero
    "concurrent"  neither precedes the other
    """
    a, b = validate_vector(a), validate_vector(b)
    smaller = larger = False
    for key in a.keys() | b.keys():
        left, right = a.get(key, 0), b.get(key, 0)
        if left < right:
            smaller = True
        elif left > right:
            larger = True
    if smaller and larger:
        return "concurrent"
    if smaller:
        return "before"
    if larger:
        return "after"
    return "equal"


class VectorClock:
    """One clock per process, shared by every helper in that process."""

    def __init__(self, node_id):
        self._node_id = _check_identity(node_id)
        self._lock = threading.Lock()
        self._vector = {node_id: 0}

    @property
    def node_id(self):
        return self._node_id

    def _bump(self, vector):
        current = vector.get(self._node_id, 0)
        if current >= MAX_COUNTER:
            raise VectorClockError("own counter would overflow")
        vector[self._node_id] = current + 1

    def tick(self):
        """Local application event. Returns a copy of the new vector."""
        with self._lock:
            self._bump(self._vector)
            return dict(self._vector)

    def send(self):
        """Send event: increment, then return the snapshot to attach."""
        with self._lock:
            self._bump(self._vector)
            return dict(self._vector)

    def receive(self, remote_vector):
        """Receive event: component-wise maximum, then increment own component."""
        remote = validate_vector(remote_vector)
        with self._lock:
            merged = dict(self._vector)           # build aside; commit only on success
            for key, value in remote.items():
                if value > merged.get(key, 0):
                    merged[key] = value
            if len(merged) > MAX_COMPONENTS:
                raise VectorClockError("merged vector would exceed %d components"
                                       % MAX_COMPONENTS)
            self._bump(merged)
            self._vector = merged
            return dict(merged)

    def snapshot(self):
        """Copy of the current vector. Reading never advances the clock."""
        with self._lock:
            return dict(self._vector)

    def __repr__(self):
        return "VectorClock(%r, %r)" % (self._node_id, self.snapshot())
