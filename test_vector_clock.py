import threading
import unittest

from vector_clock import (MAX_COUNTER, VectorClock, VectorClockError,
                          compare_vectors, decode_vector, encode_vector,
                          validate_vector)


class ClockOperations(unittest.TestCase):
    def test_new_clock_starts_at_zero_and_reading_does_not_advance(self):
        clock = VectorClock("a:1")
        self.assertEqual(clock.snapshot(), {"a:1": 0})
        clock.snapshot(); clock.snapshot()
        self.assertEqual(clock.snapshot(), {"a:1": 0})
        self.assertEqual(repr(clock), "VectorClock('a:1', {'a:1': 0})")

    def test_tick_and_send_each_increment_once(self):
        clock = VectorClock("a")
        self.assertEqual(clock.tick(), {"a": 1})
        self.assertEqual(clock.send(), {"a": 2})
        self.assertEqual(clock.tick(), {"a": 3})
        self.assertEqual(clock.snapshot(), {"a": 3})

    def test_receive_takes_maximum_then_increments_own(self):
        clock = VectorClock("a")
        clock.tick(); clock.tick()                    # a=2
        result = clock.receive({"a": 1, "b": 5})      # remote a is older
        self.assertEqual(result, {"a": 3, "b": 5})
        result = clock.receive({"a": 10, "c": 1})     # remote knows more of a
        self.assertEqual(result, {"a": 11, "b": 5, "c": 1})

    def test_missing_components_are_zero(self):
        clock = VectorClock("a")
        self.assertEqual(clock.receive({"b": 1}), {"a": 1, "b": 1})
        self.assertEqual(compare_vectors({"a": 1}, {"a": 1, "b": 0}), "equal")
        self.assertEqual(compare_vectors({}, {"a": 0}), "equal")
        self.assertEqual(compare_vectors({}, {"a": 1}), "before")

    def test_learned_components_are_never_removed(self):
        clock = VectorClock("a")
        clock.receive({"b": 3})
        clock.receive({})
        clock.receive({"c": 1})
        self.assertEqual(clock.snapshot(), {"a": 3, "b": 3, "c": 1})

    def test_counters_never_go_backwards_on_receive(self):
        clock = VectorClock("a")
        clock.receive({"b": 9})
        self.assertEqual(clock.receive({"b": 2})["b"], 9)

    def test_returned_vectors_are_copies(self):
        clock = VectorClock("a")
        for result in (clock.tick(), clock.send(), clock.receive({"b": 1}),
                       clock.snapshot()):
            result["a"] = 999
            result["zzz"] = 1
        self.assertEqual(clock.snapshot(), {"a": 3, "b": 1})

    def test_receive_does_not_keep_the_callers_dict(self):
        clock = VectorClock("a")
        remote = {"b": 1}
        clock.receive(remote)
        remote["b"] = 50
        self.assertEqual(clock.snapshot()["b"], 1)
        self.assertEqual(remote, {"b": 50})

    def test_failed_receive_leaves_clock_unchanged(self):
        clock = VectorClock("a")
        clock.tick()
        before = clock.snapshot()
        for bad in ({"b": -1}, {"b": True}, {"b": 1.0}, {"": 1}, {1: 1},
                    [], None, "x"):
            with self.assertRaises(VectorClockError):
                clock.receive(bad)
        self.assertEqual(clock.snapshot(), before)

    def test_own_counter_overflow_is_rejected_without_change(self):
        clock = VectorClock("a")
        clock.receive({"a": MAX_COUNTER - 1})          # merge, +1: own is now at the limit
        for operation in (clock.tick, clock.send):
            with self.assertRaises(VectorClockError):
                operation()
        with self.assertRaises(VectorClockError):
            clock.receive({"b": 1})
        self.assertEqual(clock.snapshot(), {"a": MAX_COUNTER})

    def test_invalid_identity_rejected(self):
        for bad in ("", None, 5, "x" * 301):
            with self.assertRaises(VectorClockError):
                VectorClock(bad)


class Comparison(unittest.TestCase):
    def test_causal_chain_is_before_and_after(self):
        a, b = VectorClock("a"), VectorClock("b")
        sent = a.send()
        received = b.receive(sent)
        self.assertEqual(compare_vectors(sent, received), "before")
        self.assertEqual(compare_vectors(received, sent), "after")
        reply = b.send()
        back = a.receive(reply)
        self.assertEqual(compare_vectors(sent, back), "before")

    def test_equal(self):
        self.assertEqual(compare_vectors({"a": 2, "b": 1}, {"b": 1, "a": 2}), "equal")

    def test_concurrent(self):
        self.assertEqual(compare_vectors({"a": 1}, {"b": 1}), "concurrent")
        self.assertEqual(compare_vectors({"a": 2, "b": 0}, {"a": 1, "b": 1}),
                         "concurrent")
        a, b = VectorClock("a"), VectorClock("b")
        self.assertEqual(compare_vectors(a.tick(), b.tick()), "concurrent")

    def test_compare_rejects_malformed(self):
        with self.assertRaises(VectorClockError):
            compare_vectors({"a": -1}, {})
        with self.assertRaises(VectorClockError):
            compare_vectors({}, "nope")

    def test_compare_does_not_mutate_inputs(self):
        a, b = {"a": 1}, {"b": 2}
        compare_vectors(a, b)
        self.assertEqual((a, b), ({"a": 1}, {"b": 2}))


class Encoding(unittest.TestCase):
    def test_round_trip_is_compact_and_deterministic(self):
        vector = {"z:1": 2, "a:1": 1}
        text = encode_vector(vector)
        self.assertEqual(text, '{"a:1":1,"z:1":2}')
        self.assertEqual(decode_vector(text), vector)
        self.assertEqual(decode_vector(text.encode()), vector)

    def test_decode_rejects_malformed(self):
        for bad in ("", "nope", "[]", '"x"', "null", '{"a":1.5}', '{"a":"1"}',
                    '{"a":-1}', '{"a":NaN}', '{"a":Infinity}', '{"a":true}',
                    '{"a":1,"a":2}', '{"":1}', "x" * 70000, b"\xff\xfe", 5, None):
            with self.subTest(bad=bad if not isinstance(bad, str) else bad[:20]):
                with self.assertRaises(VectorClockError):
                    decode_vector(bad)

    def test_validate_returns_copy(self):
        original = {"a": 1}
        checked = validate_vector(original)
        checked["a"] = 2
        self.assertEqual(original, {"a": 1})

    def test_too_many_components_rejected(self):
        with self.assertRaises(VectorClockError):
            validate_vector({"n%d" % i: 1 for i in range(1025)})


class Concurrency(unittest.TestCase):
    def test_many_threads_tick_one_clock_without_lost_updates(self):
        clock = VectorClock("a")
        threads, per_thread = 8, 500

        def work():
            for _ in range(per_thread):
                clock.tick()

        pool = [threading.Thread(target=work) for _ in range(threads)]
        for t in pool: t.start()
        for t in pool: t.join()
        self.assertEqual(clock.snapshot(), {"a": threads * per_thread})

    def test_mixed_tick_send_receive_count_every_event(self):
        clock = VectorClock("a")
        seen = []
        lock = threading.Lock()

        def work(kind):
            for i in range(300):
                if kind == 0:
                    result = clock.tick()
                elif kind == 1:
                    result = clock.send()
                else:
                    result = clock.receive({"b": i})
                with lock:
                    seen.append(result["a"])

        pool = [threading.Thread(target=work, args=(k % 3,)) for k in range(9)]
        for t in pool: t.start()
        for t in pool: t.join()
        self.assertEqual(clock.snapshot()["a"], 9 * 300)
        self.assertEqual(sorted(seen), list(range(1, 9 * 300 + 1)))  # each value once
        self.assertEqual(clock.snapshot()["b"], 299)


if __name__ == "__main__":
    unittest.main()
