"""Bounded offline linearizability checking for completed MiniKV operations.

The model starts empty and covers independent keys with the data limit disabled.
It accepts successful PUTs and completed GET/DELETE hits or misses. Timeouts,
unknown write outcomes, capacity rejection, and multi-key transactions are outside
this model and must never be silently discarded from a recorded test history.

The returned per-key operation ID sequences are witnesses, not response order.
The locality of linearizability permits independent key checking for this model.
"""

from bisect import bisect_left
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Operation:
    id: int
    method: str
    key: str
    value: Optional[str]
    started: int
    finished: int
    status: int
    body: bytes


class InvalidHistory(ValueError):
    """The recording is malformed or contains an unsupported outcome."""


class NotLinearizable(AssertionError):
    """No sequential history matches the responses and real-time constraints."""


class SearchBudgetExceeded(RuntimeError):
    """The configured bound prevents a verdict; this is never a passing check."""


def _validate(operation: Operation) -> None:
    if not isinstance(operation, Operation):
        raise InvalidHistory("every history entry must be an Operation")
    for field in ("id", "started", "finished", "status"):
        if type(getattr(operation, field)) is not int:
            raise InvalidHistory("operation {}: {} must be an integer".format(
                operation.id, field))
    if operation.started > operation.finished:
        raise InvalidHistory("operation {} finishes before it starts".format(operation.id))
    if not isinstance(operation.key, str):
        raise InvalidHistory("operation {} has a non-string key".format(operation.id))
    if operation.method not in ("PUT", "GET", "DELETE"):
        raise InvalidHistory("operation {} has an unsupported method".format(operation.id))
    if not isinstance(operation.body, bytes):
        raise InvalidHistory("operation {} response body must be bytes".format(operation.id))
    if operation.method == "PUT":
        if not isinstance(operation.value, str):
            raise InvalidHistory("operation {} PUT value must be a string".format(operation.id))
        try:
            operation.value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise InvalidHistory("operation {} PUT value is not UTF-8".format(operation.id)) from error
        valid_response = operation.status == 200 and operation.body == b"OK\n"
    else:
        if operation.value is not None:
            raise InvalidHistory("operation {} only PUT takes a value".format(operation.id))
        valid_response = operation.status == 404 and operation.body == b"NOT_FOUND\n"
        if operation.status == 200:
            if operation.method == "DELETE":
                valid_response = operation.body == b"OK\n"
            else:
                valid_response = (operation.body.startswith(b"VALUE ")
                                  and operation.body.endswith(b"\n")
                                  and len(operation.body) >= 7)
    if not valid_response:
        raise InvalidHistory("operation {} has an unsupported HTTP response: {} {!r}".format(
            operation.id, operation.status, operation.body))


def check_history(operations: Iterable[Operation], *, max_states: int = 100000,
                  max_operations: int = 1000) -> Dict[str, Tuple[int, ...]]:
    """Return a legal operation ID order for each key, or raise explicitly.

    A response ending strictly before another invocation starts must precede it;
    equal timestamps impose no order. Input iteration order has no significance.
    Search uses an explicit stack and memoizes failed (completed set, value)
    states. ``max_states`` counts states entered across *all* keys, including
    initial and terminal states. ``max_operations`` also bounds preprocessing.
    Exhausting either bound raises SearchBudgetExceeded, never NotLinearizable.

    PUT's ``value`` is the submitted string; GET/DELETE use None. HTTP POST is
    recorded as the model operation PUT. GET values come only from response bytes.
    Empty strings, embedded newlines, and UTF-8 are compared without normalization.
    """
    if type(max_states) is not int or max_states < 1:
        raise ValueError("max_states must be a positive integer")
    if type(max_operations) is not int or max_operations < 1:
        raise ValueError("max_operations must be a positive integer")

    by_key = {}  # type: Dict[str, List[Operation]]
    ids = set()
    for operation in operations:
        if len(ids) >= max_operations:
            raise SearchBudgetExceeded("history exceeds {} operations".format(max_operations))
        _validate(operation)
        if operation.id in ids:
            raise InvalidHistory("duplicate operation ID {}".format(operation.id))
        ids.add(operation.id)
        by_key.setdefault(operation.key, []).append(operation)

    witnesses = {}  # type: Dict[str, Tuple[int, ...]]
    entered_states = 0

    def enter_state(key: str) -> None:
        nonlocal entered_states
        if entered_states >= max_states:
            raise SearchBudgetExceeded("search exceeded {} states while checking key {!r}".format(
                max_states, key))
        entered_states += 1

    for key, entries in by_key.items():
        # Finish ordering is only a search heuristic. Overlapping calls may need
        # the reverse order to explain a read, so every eligible branch is tried.
        ordered = sorted(entries, key=lambda op: (op.finished, op.started, op.id))
        finishes = [op.finished for op in ordered]
        predecessors = [(1 << bisect_left(finishes, op.started)) - 1 for op in ordered]
        writes = [op.value.encode("utf-8") if op.method == "PUT" else None
                  for op in ordered]
        complete = (1 << len(ordered)) - 1
        failed = set()
        # Frames contain completed bitset, stored bytes (None means absent), and
        # next candidate index. Explicit frames avoid Python recursion limits.
        stack = [[0, None, 0]]
        path = []  # type: List[int]
        enter_state(key)
        while stack:
            frame = stack[-1]
            done, value, candidate = frame
            if done == complete:
                witnesses[key] = tuple(path)
                break
            descended = False
            for index in range(candidate, len(ordered)):
                frame[2] = index + 1
                bit = 1 << index
                if done & bit or predecessors[index] & ~done:
                    continue
                operation = ordered[index]
                next_value = value
                if operation.method == "PUT":
                    next_value = writes[index]
                elif operation.method == "DELETE":
                    if (operation.status == 200) != (value is not None):
                        continue
                    next_value = None
                else:
                    observed = operation.body[6:-1] if operation.status == 200 else None
                    if observed != value:
                        continue
                state = (done | bit, next_value)
                if state in failed:
                    continue
                enter_state(key)
                stack.append([state[0], state[1], 0])
                path.append(operation.id)
                descended = True
                break
            if not descended:
                failed.add((done, value))
                stack.pop()
                if path:
                    path.pop()
        else:
            raise NotLinearizable("key {!r} has no legal order for operation IDs {}".format(
                key, [op.id for op in ordered]))
    return witnesses
