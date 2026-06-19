"""_supervise re-spawns dead worker slots in run-forever mode (no real processes)."""

from __future__ import annotations

from live import main


class FakeProc:
    """Minimal mp.Process stand-in. Reports dead unless `alive` is set."""

    _pids = iter(range(1000, 100000))

    def __init__(self, alive: bool = False) -> None:
        self.alive = alive
        self.pid = next(FakeProc._pids)
        self.exitcode = None if alive else 1
        self.terminated = False
        self.joined = False

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminated = True

    def join(self) -> None:
        self.joined = True


def test_supervise_respawns_dead_worker() -> None:
    """A slot whose process is dead is re-spawned on the next sweep."""
    spawned: list[tuple] = []

    def fake_spawn(spec):
        spawned.append(spec)
        return FakeProc(alive=False)  # always "dies" -> must be re-spawned each sweep

    clock = [0.0]

    def fake_now() -> float:
        return clock[0]

    def fake_sleep(_secs: float) -> None:
        clock[0] += 100.0  # jump past any per-slot backoff window

    spec = (lambda: None, (), "fake-worker")

    # Run exactly 3 sweeps, then stop.
    main._supervise(
        [spec],
        spawn=fake_spawn,
        poll=0.0,
        stop_after=lambda it: it >= 3,
        sleep=fake_sleep,
        now=fake_now,
    )

    # 1 initial start + one re-spawn per sweep (3 sweeps) = 4 spawns of the slot.
    assert len(spawned) == 4
    assert all(s is spec for s in spawned)


def test_supervise_leaves_live_worker_alone() -> None:
    """A slot whose process stays alive is never re-spawned."""
    spawned: list[tuple] = []

    def fake_spawn(spec):
        spawned.append(spec)
        return FakeProc(alive=True)  # stays alive -> no re-spawn

    spec = (lambda: None, (), "live-worker")
    main._supervise(
        [spec],
        spawn=fake_spawn,
        poll=0.0,
        stop_after=lambda it: it >= 5,
        sleep=lambda _s: None,
        now=lambda: 0.0,
    )

    assert len(spawned) == 1  # only the initial start


def test_supervise_terminates_all_on_exit() -> None:
    """On loop exit every worker is terminated and joined (shutdown path)."""
    procs: list[FakeProc] = []

    def fake_spawn(spec):
        p = FakeProc(alive=True)
        procs.append(p)
        return p

    specs = [(lambda: None, (), "a"), (lambda: None, (), "b")]
    main._supervise(
        specs,
        spawn=fake_spawn,
        poll=0.0,
        stop_after=lambda it: it >= 1,
        sleep=lambda _s: None,
        now=lambda: 0.0,
    )

    assert len(procs) == 2
    assert all(p.terminated and p.joined for p in procs)
