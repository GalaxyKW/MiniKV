"""Independent examples and a small exhaustive oracle for the history checker."""

from dataclasses import replace
from itertools import permutations, product
import unittest

from history_checker import (
    InvalidHistory, NotLinearizable, Operation, SearchBudgetExceeded, check_history,
)


def put(identifier, value, start, finish, key="k"):
    return Operation(identifier, "PUT", key, value, start, finish, 200, b"OK\n")


def get(identifier, value, start, finish, key="k"):
    return Operation(identifier, "GET", key, None, start, finish,
                     404 if value is None else 200,
                     b"NOT_FOUND\n" if value is None else b"VALUE " + value.encode() + b"\n")


def delete(identifier, hit, start, finish, key="k"):
    return Operation(identifier, "DELETE", key, None, start, finish,
                     200 if hit else 404, b"OK\n" if hit else b"NOT_FOUND\n")


def replay_witness(test, history, witnesses):
    """Check a returned witness directly using a dictionary and pairwise times."""
    by_id = {operation.id: operation for operation in history}
    test.assertEqual(set(witnesses), {operation.key for operation in history})
    for key, order in witnesses.items():
        expected_ids = [operation.id for operation in history if operation.key == key]
        test.assertCountEqual(order, expected_ids)
        positions = {identifier: index for index, identifier in enumerate(order)}
        for first in expected_ids:
            for second in expected_ids:
                if by_id[first].finished < by_id[second].started:
                    test.assertLess(positions[first], positions[second])
        store = {}
        for identifier in order:
            operation = by_id[identifier]
            if operation.method == "PUT":
                store[key] = operation.value
                expected = (200, b"OK\n")
            elif key not in store:
                expected = (404, b"NOT_FOUND\n")
            elif operation.method == "DELETE":
                del store[key]
                expected = (200, b"OK\n")
            else:
                expected = (200, b"VALUE " + store[key].encode() + b"\n")
            test.assertEqual((operation.status, operation.body), expected)


def exhaustive_oracle(history):
    """Deliberately simple factorial oracle, independent of the bitset search."""
    for ordering in permutations(history):
        positions = {operation.id: index for index, operation in enumerate(ordering)}
        if any(first.finished < second.started and positions[first.id] > positions[second.id]
               for first in history for second in history):
            continue
        contents = {}
        for operation in ordering:
            if operation.method == "PUT":
                contents[operation.key] = operation.value
            elif operation.method == "GET":
                expected = ((200, b"VALUE " + contents[operation.key].encode() + b"\n")
                            if operation.key in contents else (404, b"NOT_FOUND\n"))
                if (operation.status, operation.body) != expected:
                    break
            elif (operation.status == 200) != (operation.key in contents):
                break
            else:
                contents.pop(operation.key, None)
        else:
            return True
    return False


class HistoryCheckerTests(unittest.TestCase):
    def assert_linearizable(self, history, **kwargs):
        history = list(history)
        witnesses = check_history(history, **kwargs)
        replay_witness(self, history, witnesses)
        return witnesses

    def test_empty_history(self):
        self.assertEqual(check_history([]), {})

    def test_overlapping_read_requires_reverse_response_order(self):
        # The read responds before the write but observes its value. Ordering by
        # response time would reject this perfectly legal overlapping history.
        history = [get(2, "new", 2, 3), put(1, "new", 1, 4)]
        self.assertEqual(self.assert_linearizable(history), {"k": (1, 2)})

    def test_backtracks_after_plausible_first_write(self):
        history = [put(1, "a", 1, 4), put(2, "b", 2, 5), get(3, "a", 6, 7)]
        self.assertEqual(self.assert_linearizable(history), {"k": (2, 1, 3)})

    def test_real_time_prevents_future_value(self):
        with self.assertRaises(NotLinearizable):
            check_history([get(1, "future", 1, 2), put(2, "future", 3, 4)])

    def test_real_time_prevents_stale_read_after_overwrite(self):
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "old", 1, 2), put(2, "new", 3, 4),
                           get(3, "old", 5, 6)])

    def test_real_time_prevents_missing_read_after_put(self):
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "present", 1, 2), get(2, None, 3, 4)])

    def test_delete_miss_does_not_create_or_remove_a_value(self):
        self.assert_linearizable([delete(1, False, 1, 2), get(2, None, 3, 4),
                                  put(3, "a", 5, 6), delete(4, True, 7, 8),
                                  delete(5, False, 9, 10), get(6, None, 11, 12)])

    def test_delete_miss_on_present_key_is_impossible(self):
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "a", 1, 2), delete(2, False, 3, 4)])

    def test_delete_hit_on_absent_key_is_impossible(self):
        with self.assertRaises(NotLinearizable):
            check_history([delete(1, True, 1, 2)])

    def test_deleted_value_cannot_reappear_without_write(self):
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "a", 1, 2), delete(2, True, 3, 4), get(3, "a", 5, 6)])

    def test_empty_value_is_distinct_from_absence(self):
        self.assert_linearizable([put(1, "", 1, 2), get(2, "", 3, 4),
                                  delete(3, True, 5, 6), get(4, None, 7, 8)])
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "", 1, 2), get(2, None, 3, 4)])

    def test_values_are_compared_without_stripping_or_decoding_loss(self):
        value = " \n中文\x00\n "
        self.assert_linearizable([put(1, value, 1, 2), get(2, value, 3, 4)])
        with self.assertRaises(NotLinearizable):
            check_history([put(1, value, 1, 2), get(2, value.strip(), 3, 4)])

    def test_boundary_equality_is_not_a_real_time_edge(self):
        # Strict '<' is intentional; this includes a zero-duration observation.
        history = [get(1, "a", 0, 1), put(2, "a", 1, 1)]
        self.assertEqual(self.assert_linearizable(history), {"k": (2, 1)})

    def test_independent_keys_have_separate_state_and_witnesses(self):
        history = [put(1, "a", 1, 2, "left"), get(2, None, 3, 4, "right"),
                   put(3, "b", 5, 8, "right"), get(4, "a", 6, 7, "left"),
                   get(5, "b", 9, 10, "right")]
        self.assertEqual(self.assert_linearizable(reversed(history)),
                         {"right": (2, 3, 5), "left": (1, 4)})
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "a", 1, 2, "left"), get(2, "a", 3, 4, "right")])

    def test_one_invalid_key_cannot_be_hidden_by_valid_keys(self):
        with self.assertRaises(NotLinearizable):
            check_history([put(1, "a", 1, 2, "good"), get(2, "missing", 3, 4, "bad")])

    def test_input_order_is_not_history_order(self):
        history = [put(1, "a", 1, 2), get(2, "a", 3, 4), delete(3, True, 5, 6)]
        for order in permutations(history):
            self.assertEqual(self.assert_linearizable(order), {"k": (1, 2, 3)})

    def test_malformed_or_unsupported_responses_are_rejected(self):
        examples = [replace(put(1, "a", 1, 2), status=503),
                    replace(put(1, "a", 1, 2), status=404, body=b"NOT_FOUND\n"),
                    replace(put(1, "a", 1, 2), body=b"OK"),
                    replace(delete(1, True, 1, 2), body=b"VALUE a\n"),
                    replace(delete(1, False, 1, 2), body=b"NOT_FOUND"),
                    replace(get(1, "a", 1, 2), body=b"OK\n"),
                    replace(get(1, "a", 1, 2), body=b"VALUE a"),
                    replace(get(1, None, 1, 2), body=b"VALUE \n"),
                    replace(get(1, "a", 1, 2), status=504)]
        for operation in examples:
            with self.subTest(operation=operation), self.assertRaises(InvalidHistory):
                check_history([operation])

    def test_invalid_recordings_are_rejected(self):
        valid = put(1, "a", 1, 2)
        examples = [replace(valid, id=True), replace(valid, started=3),
                    replace(valid, finished=None), replace(valid, status="200"),
                    replace(valid, key=None), replace(valid, method="POST"),
                    replace(valid, value=None), replace(valid, value="\ud800"),
                    replace(valid, body="OK\n"), replace(get(1, "a", 1, 2), value="a")]
        for operation in examples:
            with self.subTest(operation=operation), self.assertRaises(InvalidHistory):
                check_history([operation])
        with self.assertRaises(InvalidHistory):
            check_history([valid, replace(valid, key="other")])
        with self.assertRaises(InvalidHistory):
            check_history([{}])

    def test_budget_exhaustion_is_not_a_consistency_verdict(self):
        with self.assertRaises(SearchBudgetExceeded):
            check_history([put(1, "a", 1, 2)], max_states=1)
        self.assert_linearizable([put(1, "a", 1, 2)], max_states=2)
        # Each independent key needs its own initial and terminal state. A
        # per-key budget reset would incorrectly let this recording pass.
        with self.assertRaises(SearchBudgetExceeded):
            check_history([put(1, "a", 1, 2, "x"), put(2, "b", 1, 2, "y")], max_states=3)
        with self.assertRaises(SearchBudgetExceeded):
            check_history([put(1, "a", 1, 2), get(2, "a", 3, 4)], max_operations=1)
        with self.assertRaises(SearchBudgetExceeded):
            check_history([put(1, "a", 1, 4), put(2, "b", 2, 5), get(3, "a", 6, 7)],
                          max_states=4)

    def test_invalid_budgets_are_rejected(self):
        for option in ("max_states", "max_operations"):
            for value in (0, -1, True, 1.5, None):
                with self.subTest(option=option, value=value), self.assertRaises(ValueError):
                    check_history([], **{option: value})

    def test_deep_sequential_history_uses_no_python_recursion(self):
        history = [put(index, str(index), index * 2, index * 2 + 1) for index in range(1050)]
        self.assertEqual(check_history(history, max_operations=1100)["k"], tuple(range(1050)))

    def test_small_histories_match_independent_exhaustive_oracle(self):
        # Cross several timing shapes and every read/delete observation. The
        # oracle enumerates total orders; it shares no checker search helpers.
        timings = [((0, 2), (3, 5), (6, 8), (9, 11)),
                   ((0, 8), (1, 4), (2, 6), (7, 9)),
                   ((0, 10), (1, 9), (2, 8), (3, 7)),
                   ((0, 3), (2, 4), (4, 6), (5, 8))]
        for times, observed, hit in product(timings, (None, "a", "b", "never-written"), (False, True)):
            history = [put(1, "a", *times[0]), put(2, "b", *times[1]),
                       get(3, observed, *times[2]), delete(4, hit, *times[3])]
            with self.subTest(times=times, observed=observed, hit=hit):
                if exhaustive_oracle(history):
                    self.assert_linearizable(history)
                else:
                    with self.assertRaises(NotLinearizable):
                        check_history(history)


if __name__ == "__main__":
    unittest.main()
