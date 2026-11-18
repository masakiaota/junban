# Modified for junban in 2026 by junban contributors.
"""Run with make test. Every queue is private and cleaned in finally blocks.

JUNBAN_TEST_ROOT can select an artifact directory. Socket paths are relative
to each test directory, so macOS sun_path limits do not depend on checkout depth.
"""
import contextlib
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import resource
import re
import select
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unicodedata
import unittest

REPO = Path(__file__).resolve().parents[1]
BIN = str(REPO / "junban")
WORKER = str(REPO / "tests" / "worker.py")
PY = sys.executable
ROOT = Path(os.getenv("JUNBAN_TEST_ROOT", str(REPO / "tests" / ".tmp"))).resolve()
ROOT.mkdir(parents=True, exist_ok=True)

# Keep phase-1's fixed 40-byte frame so old servers can reject the version.
# The estimate is a separate eight-byte NEWJOB payload, after negotiation.
MESSAGE = struct.Struct("=10i")
_enum = (REPO / "main.h").read_text().split("enum msg_types")[1].split("{")[1].split("}")[0]
TYPES = {name.strip(): i for i, name in enumerate(_enum.split(","))}
PROTOCOL = int(re.search(r"PROTOCOL_VERSION=(\d+)", (REPO / "main.h").read_text()).group(1))


def send_message(sock, kind, *values):
    sock.sendall(MESSAGE.pack(TYPES[kind], *values, *([0] * (9 - len(values)))))
    if kind == "NEWJOB":
        sock.sendall(struct.pack("=Q", 0))


def receive_message(sock):
    data = b""
    while len(data) < MESSAGE.size:
        chunk = sock.recv(MESSAGE.size - len(data))
        if not chunk:
            raise AssertionError("unexpected protocol EOF")
        data += chunk
    return MESSAGE.unpack(data)


def connect_raw(q):
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(3)
    s.connect(os.path.relpath(q.path / "s"))
    send_message(s, "GET_VERSION")
    assert receive_message(s)[:2] == (TYPES["VERSION"], PROTOCOL)
    return s


def eventually(fn, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition did not become true")


def alive(pid):
    r = subprocess.run(["ps", "-p", str(pid), "-o", "stat="],
                       capture_output=True, text=True)
    return bool(r.stdout.strip()) and not r.stdout.strip().startswith("Z")


def servers():
    r = subprocess.run(["ps", "-axo", "pid=,pgid=,args="],
                       check=True, capture_output=True, text=True)
    found = set()
    for line in r.stdout.splitlines():
        fields = line.strip().split(None, 2)
        if (len(fields) == 3 and fields[0] == fields[1]
                and fields[2].startswith(BIN + " ")):
            found.add(int(fields[0]))
    return found


class Queue:
    def __init__(self, slots=4):
        self.path = Path(tempfile.mkdtemp(prefix="q-", dir=ROOT))
        self.env = dict(os.environ, JUNBAN_SOCKET="s", JUNBAN_SLOTS=str(slots))
        self.clients = []
        self.groups = set()
        self.server_pids = set()
        self.before_servers = servers()

    def popen(self, *args, **kw):
        env = kw.pop("env", self.env)
        p = subprocess.Popen([BIN, *map(str, args)], cwd=kw.pop("cwd", self.path),
                             env=env, stdin=kw.pop("stdin", subprocess.DEVNULL),
                             stdout=kw.pop("stdout", subprocess.PIPE),
                             stderr=kw.pop("stderr", subprocess.PIPE), **kw)
        self.clients.append(p)
        return p

    def call(self, *args, **kw):
        data = kw.pop("input", None)
        p = self.popen(*args, **kw)
        out, err = p.communicate(data, timeout=8)
        return p.returncode, out, err

    def rows(self):
        code, out, err = self.call("ls")
        assert code == (0 if out else 1) and err == b"", (code, out, err)
        rows = []
        for line in out.decode().splitlines():
            fields = line.split()
            if fields and fields[0] in ("running", "queued"):
                rows.append((int(fields[1].split("=")[1]), fields[0]))
        return rows

    def wait_rows(self, count):
        return eventually(lambda: (r if len(r := self.rows()) == count else None))

    def start(self, name, cpus=None, tree=False, estimate=None, gpus=None, **kwargs):
        args = ["run"] + (["-c", str(cpus)] if cpus else [])
        if gpus is not None:
            args += ["-g", str(gpus)]
        if estimate:
            args += ["--estimate", estimate]
        p = self.popen(*args, PY, WORKER, "tree" if tree else "hold",
                       name + ".pids", name + ".release", **kwargs)
        return p

    def pids(self, name):
        record = self.path / (name + ".pids")
        def read():
            try:
                return json.loads(record.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                return None
        pids = eventually(read)
        self.groups.add(pids[0])
        return pids

    def release(self, name):
        (self.path / (name + ".release")).touch()

    def server(self):
        pid = eventually(lambda: int((self.path / "s.lock").read_text().strip() or "0"))
        self.server_pids.add(pid)
        return pid

    def close(self):
        # Record all trees, including those started just before an assertion.
        recorded = []
        for file in self.path.glob("*.pids"):
            with contextlib.suppress(ValueError):
                pids = json.loads(file.read_text())
                recorded.extend(pids)
                self.groups.add(pids[0])
        for p in self.clients:
            if p.poll() is None:
                p.terminate()
        for pgid in self.groups:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
        for p in self.clients:
            try:
                p.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.communicate(timeout=3)
        self.server_pids |= servers() - self.before_servers
        lock = self.path / "s.lock"
        if lock.exists() and lock.read_text().strip():
            self.server_pids.add(int(lock.read_text()))
        for pid in self.server_pids:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGTERM)
        for pid in self.server_pids | set(recorded):
            eventually(lambda pid=pid: not alive(pid))
        shutil.rmtree(self.path)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class QueueTests(unittest.TestCase):
    def own_failure(self, result, reason=None):
        code, out, err = result
        self.assertEqual(code, 125, result)
        self.assertEqual(out, b"")
        self.assertTrue(err.startswith(b"junban: "), result)
        lines = err.splitlines()
        self.assertTrue(all(line.startswith(b"junban: ") for line in lines), result)
        self.assertTrue(all(b"queued" in line or b"running id=" in line
                            for line in lines[:-1]), result)
        if reason:
            self.assertIn(reason.encode(), err)

    def test_transparency(self):
        with Queue() as q:
            server_env = dict(q.env, MARK="server", SERVER_ONLY="server")
            self.assertEqual(q.call("ls", env=server_env)[0], 1)
            for name in ("a", "b"):
                directory = q.path / name
                directory.mkdir()
                env = dict(q.env, JUNBAN_SOCKET="../s", MARK="caller",
                           CALLER_ONLY="caller", POSIXLY_CORRECT="original")
                env.pop("SERVER_ONLY", None)
                data = b"pipe-data\nsecond line\n"
                code, out, err = q.call("run", PY, WORKER, "transparent",
                                        cwd=directory, env=env,
                                        stdin=subprocess.PIPE, input=data)
                self.assertEqual(code, 0, err)
                self.assertEqual(json.loads(out), {
                    "cwd": str(directory), "mark": "caller", "only": "caller",
                    "server_only": None, "posix": "original", "stdin": data.decode()})
                self.assertEqual(err, b"separate stderr\n")
            source = q.path / "input"
            source.write_bytes(b"file input\n")
            with source.open("rb") as f:
                self.assertEqual(q.call("run", "cat", stdin=f),
                                 (0, b"file input\n", b""))
            p = q.popen("run", PY, WORKER, "stream")
            for stream, expected in ((p.stdout, b"O1\n"), (p.stderr, b"E1\n")):
                self.assertTrue(select.select([stream], [], [], 2)[0])
                self.assertEqual(os.read(stream.fileno(), 3), expected)
                self.assertIsNone(p.poll())
            self.assertEqual(p.communicate(timeout=3), (b"O2\n", b"E2\n"))
            self.assertEqual(p.returncode, 0)

    def test_nested_run_is_refused(self):
        with Queue() as q:
            # Record execution while keeping the inner invocation `run true`.
            command = q.path / "true"
            command.write_text("#!/bin/sh\n: > ran\n")
            command.chmod(0o755)
            env = dict(q.env, PATH=str(q.path) + os.pathsep + os.defpath)
            result = q.call("run", PY, "-c",
                            "import subprocess, sys; "
                            "sys.exit(subprocess.run([sys.argv[1], 'run', 'true'], "
                            "timeout=3).returncode)", BIN, env=env)
            self.assertEqual(result[0], 125, result)
            self.assertFalse((q.path / "ran").exists())
            self.assertEqual(q.call("run", "true", env=env), (0, b"", b""))
            self.assertTrue((q.path / "ran").exists())

    def test_tty(self):
        with Queue() as q:
            q.call("ls")
            master, slave = pty.openpty()
            def controlling_terminal():
                os.setsid()
                fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                os.tcsetpgrp(0, os.getpgrp())
            try:
                p = q.popen("run", PY, WORKER, "tty", stdin=slave, stdout=slave,
                            stderr=slave, preexec_fn=controlling_terminal)
                os.close(slave)
                slave = -1
                output = b""
                end = time.monotonic() + 5
                while b"FG:1" not in output and time.monotonic() < end:
                    if select.select([master], [], [], 0.1)[0]:
                        output += os.read(master, 4096)
                self.assertIn(b"TTY:1,1,1", output)
                self.assertIn(b"FG:1", output)
                os.write(master, b"terminal-input\n")
                while b"READ:terminal-input" not in output and time.monotonic() < end:
                    if select.select([master], [], [], 0.1)[0]:
                        output += os.read(master, 4096)
                self.assertIn(b"READ:terminal-input", output)
                self.assertEqual(p.wait(timeout=3), 0)
            finally:
                os.close(master)
                if slave >= 0:
                    os.close(slave)

    def test_exit_codes(self):
        with Queue() as q:
            for code in (0, 7, 125, 126, 127, 200, 255):
                self.assertEqual(q.call("run", "sh", "-c", "exit " + str(code)),
                                 (code, b"", b""))
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGKILL):
                self.assertEqual(q.call("run", "sh", "-c", "kill -%d $$" % sig),
                                 (128 + sig, b"", b""))
            self.assertEqual(q.call("run", "no-such-junban-command")[0], 127)
            script = q.path / "not-executable"
            script.write_text("#!/bin/sh\nexit 0\n")
            self.assertEqual(q.call("run", "./not-executable")[0], 126)
            self.assertEqual(q.call("run", ".")[0], 126)
            self.own_failure(q.call("kill", "99999"), "unknown")

    def test_usage_and_run_options(self):
        with Queue() as q:
            for args in ([], ["status"], ["python", "x.py"], ["-l"], ["run"],
                         ["kill"], ["kill", "-1"], ["ls", "x"], ["run", "-f", "true"]):
                self.own_failure(q.call(*args), "run")
            self.assertFalse((q.path / "s").exists())
            for sub in ("help", "--help", "version", "--version"):
                code, out, err = q.call(sub)
                self.assertEqual(code, 0)
                self.assertTrue(out)
                self.assertEqual(err, b"")
            for opts in (["-c", "4"], ["--cpus", "4"], ["-g", "1"], ["--gpus", "0"],
                         ["-c", "1", "-g", "1"], ["--"], []):
                self.assertEqual(q.call("run", *opts, PY, "-c", "print(1)"),
                                 (0, b"1\n", b""))
            self.assertEqual(q.rows(), [])

    def test_slots_and_fifo(self):
        with Queue() as q:
            for value in ("x", "1x", "0", "-1", "+1", " 1", "1.5", "999999999999", ""):
                self.own_failure(q.call("run", "-c", value, "true"))
            self.assertFalse((q.path / "s").exists())
            self.own_failure(q.call("run", "-c", "5", "true"), "CPUs")
            self.assertEqual(q.rows(), [])
            a = q.start("a", 3)
            q.pids("a")
            b = q.start("b")  # unspecified = all four slots
            q.wait_rows(2)
            c = q.start("c", 1)
            rows = q.wait_rows(3)
            self.assertEqual([r[1] for r in rows], ["running", "queued", "queued"])
            time.sleep(0.15)
            self.assertFalse((q.path / "b.pids").exists())
            self.assertFalse((q.path / "c.pids").exists())
            q.release("a")
            self.assertEqual(a.wait(timeout=3), 0)
            q.pids("b")
            self.assertFalse((q.path / "c.pids").exists())
            q.release("b")
            self.assertEqual(b.wait(timeout=3), 0)
            q.pids("c")
            q.release("c")
            self.assertEqual(c.wait(timeout=3), 0)

    def test_gpu_options(self):
        with Queue() as q:
            for value in ("x", "-1", "1x", ""):
                self.own_failure(q.call("run", "-g", value, "true"))
            for args in (["-g", "1", "-g", "1"], ["--gpus", "0", "-g", "0"], ["-g"]):
                self.own_failure(q.call("run", *args, "true"))
            self.assertFalse((q.path / "s").exists())
            self.own_failure(q.call("run", "-g", "2", "true"), "GPUs")
            self.own_failure(q.call("run", "--detach", "-c", "1", "-g", "2", "true"), "GPUs")
            # Each option sets only its own resource.
            a = q.start("a", gpus=1)
            q.pids("a")
            self.assertRegex(q.call("ls")[1].decode(),
                             r"^running id=0 dir=\S+ cpus=all\(4\) gpus=1 elapsed=")
            self.assertEqual(q.call("ls", "--summary"),
                             (0, b"cpus 4/4 gpus 1/1 running 1 queued 0\n", b""))
            q.release("a")
            self.assertEqual(a.wait(timeout=3), 0)
            b = q.start("b", 2)
            q.pids("b")
            self.assertRegex(q.call("ls")[1].decode(), r"^running id=1 dir=\S+ cpus=2 gpus=0 ")
            q.release("b")
            self.assertEqual(b.wait(timeout=3), 0)

    def test_per_resource_fifo(self):
        with Queue(4) as q:
            def states():
                return dict(q.rows())
            g1 = q.start("g1", 1, gpus=1)
            q.pids("g1")
            env = dict(q.env, JUNBAN_NOTIFY_SEC="60")
            g2 = q.start("g2", 1, gpus=1, env=env)
            notice = read_until(g2.stderr, lambda data: b"ahead" in data)
            self.assertIn(b"id=1 queued, cpus=1, gpus=1, 1 ahead, waiting for gpus", notice)
            # A CPU job passes a job that waits only for the GPU.
            c = q.start("c", 2)
            q.pids("c")
            self.assertEqual(states(), {0: "running", 1: "queued", 2: "running"})
            self.assertEqual(q.call("ls", "--summary"),
                             (0, b"cpus 3/4 gpus 1/1 running 2 queued 1\n", b""))
            # g2 reserves the last free CPU, so a later one-CPU job waits.
            d = q.start("d", 1)
            q.wait_rows(4)
            time.sleep(0.15)
            self.assertFalse((q.path / "d.pids").exists())
            q.release("g1")
            self.assertEqual(g1.wait(timeout=3), 0)
            q.pids("g2")
            self.assertEqual(states(), {1: "running", 2: "running", 3: "queued"})
            q.release("c")
            self.assertEqual(c.wait(timeout=3), 0)
            q.pids("d")
            # A job that needs all CPUs keeps everything behind it waiting,
            # GPU jobs too, so it cannot starve.
            e = q.start("e")
            q.wait_rows(3)
            f = q.start("f", 1, gpus=1)
            q.wait_rows(4)
            time.sleep(0.15)
            self.assertFalse((q.path / "f.pids").exists())
            q.release("g2")
            q.release("d")
            self.assertEqual(g2.wait(timeout=3), 0)
            self.assertEqual(d.wait(timeout=3), 0)
            q.pids("e")
            self.assertFalse((q.path / "f.pids").exists())
            q.release("e")
            self.assertEqual(e.wait(timeout=3), 0)
            q.pids("f")
            q.release("f")
            self.assertEqual(f.wait(timeout=3), 0)

    def test_parallel_slots(self):
        with Queue() as q:
            a, b = q.start("a", 3), q.start("b", 1)
            q.pids("a")
            q.pids("b")
            self.assertEqual([r[1] for r in q.rows()], ["running", "running"])
            q.release("a")
            q.release("b")
            self.assertEqual(a.wait(timeout=3), 0)
            self.assertEqual(b.wait(timeout=3), 0)

    def test_detected_slots_and_override(self):
        with Queue() as q:
            env = dict(q.env)
            env.pop("JUNBAN_SLOTS")
            code, out, err = q.call("ls", "--summary", env=env)
            self.assertEqual((code, err), (0, b""))
            self.assertIn(("/%d " % os.cpu_count()).encode(), out)
        with Queue(4) as q:
            q.call("ls")
            env = dict(q.env, JUNBAN_SLOTS="1")
            self.assertEqual(q.call("run", "-c", "4", "true", env=env), (0, b"", b""))
        for value in ("0", "-1", "bad", "99999999999999999999"):
            with Queue() as q:
                env = dict(q.env, JUNBAN_SLOTS=value)
                self.own_failure(q.call("run", "true", env=env))
                self.assertFalse((q.path / "s").exists())

    def test_socket_errors_and_legacy_environment(self):
        with Queue() as q:
            env = dict(q.env, JUNBAN_SOCKET="x" * 200)
            self.own_failure(q.call("run", "true", env=env), "too long")
            env = dict(q.env, JUNBAN_SOCKET="missing/s")
            self.own_failure(q.call("run", "true", env=env))
            (q.path / "file").write_text("preserve")
            env = dict(q.env, JUNBAN_SOCKET="file")
            self.own_failure(q.call("run", "true", env=env))
            self.assertEqual((q.path / "file").read_text(), "preserve")
            env = dict(q.env, TS_SOCKET="wrong", TS_SLOTS="1", TS_MAXCONN="1",
                       TS_ENV="touch legacy-env", TS_ONFINISH="touch legacy-hook",
                       TS_SAVELIST="legacy-save", TMPDIR="missing")
            self.assertEqual(q.call("run", "-c", "4", "true", env=env), (0, b"", b""))
            self.assertFalse((q.path / "wrong").exists())
            self.assertEqual(q.call("shutdown", env=env), (0, b"", b""))
            eventually(lambda: not (q.path / "s").exists())
            for name in ("legacy-env", "legacy-hook", "legacy-save"):
                self.assertFalse((q.path / name).exists())

    def test_client_signals_waiting(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGKILL):
            with self.subTest(signal=sig), Queue(1) as q:
                blocker = q.start("blocker", tree=True)
                pids = q.pids("blocker")
                waiter = q.start("waiter", tree=True)
                q.wait_rows(2)
                waiter.send_signal(sig)
                self.assertEqual(waiter.wait(timeout=3),
                                 -sig if sig == signal.SIGKILL else 128 + sig)
                eventually(lambda: len(q.rows()) == 1)
                self.assertFalse((q.path / "waiter.pids").exists())
                self.assertEqual(q.call("kill", q.rows()[0][0]), (0, b"", b""))
                self.assertEqual(blocker.wait(timeout=3), 143)
                for pid in pids:
                    eventually(lambda pid=pid: not alive(pid))

    def test_client_signals_running(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGKILL):
            with self.subTest(signal=sig), Queue(1) as q:
                p = q.start("tree", tree=True)
                pids = q.pids("tree")
                next_job = q.popen("run", PY, WORKER, "event", "next")
                q.wait_rows(2)
                p.send_signal(sig)
                self.assertEqual(p.wait(timeout=3),
                                 -sig if sig == signal.SIGKILL else 128 + sig)
                self.assertEqual(next_job.wait(timeout=3), 0)
                for pid in pids:
                    eventually(lambda pid=pid: not alive(pid))
                self.assertEqual(q.rows(), [])

    def test_kill_running_and_first_queued(self):
        with Queue(4) as q:
            p = q.start("running", 3, tree=True)
            pids = q.pids("running")
            first = q.start("first")  # first queued job blocks the one-slot job
            q.wait_rows(2)
            later = q.start("later", 1)
            rows = q.wait_rows(3)
            self.assertEqual(q.call("kill", rows[1][0]), (0, b"", b""))
            out, err = first.communicate(timeout=3)
            self.own_failure((first.returncode, out, err), "removed")
            self.assertFalse((q.path / "first.pids").exists())
            q.pids("later")
            q.release("later")
            self.assertEqual(later.wait(timeout=3), 0)
            self.assertEqual(q.call("kill", rows[0][0]), (0, b"", b""))
            self.assertEqual(p.wait(timeout=3), 143)
            for pid in pids:
                eventually(lambda pid=pid: not alive(pid))

    def test_kill_queued_at_list_head(self):
        with Queue(1) as q:
            q.call("ls")
            # Admin connects first. Descending server traversal processes the
            # later running job's ENDJOB before this admin's KILL_JOB.
            with connect_raw(q) as admin:
                p = q.start("running")
                q.pids("running")
                waiting = q.start("waiting")
                rows = q.wait_rows(2)
                # Hold the server in a partial read so ENDJOB and KILL_JOB
                # are both ready in the following select snapshot.
                with connect_raw(q) as guard:
                    message = MESSAGE.pack(TYPES["GET_VERSION"], *([0] * 9))
                    guard.sendall(message[:4])
                    time.sleep(0.1)
                    q.release("running")
                    self.assertEqual(p.wait(timeout=3), 0)
                    send_message(admin, "KILL_JOB", rows[1][0])
                    guard.sendall(message[4:])
                    self.assertEqual(receive_message(guard)[0], TYPES["VERSION"])
                self.assertEqual(receive_message(admin)[0], TYPES["KILL_JOB_OK"])
                out, err = waiting.communicate(timeout=3)
                self.own_failure((waiting.returncode, out, err), "removed")
                self.assertFalse((q.path / "waiting.pids").exists())
                eventually(lambda: not q.rows())

    def test_disconnected_submission(self):
        with Queue() as q:
            q.call("ls")
            server = q.server()
            with connect_raw(q) as s:
                send_message(s, "NEWJOB", 1000, 0, 0, 0, 0, 0, -1, 0, 0)
                s.sendall(b"incomplete")
            self.assertEqual(q.call("run", "true"), (0, b"", b""))
            self.assertEqual(q.server(), server)
            self.assertEqual(q.rows(), [])

    def test_client_dies_before_exec_ack(self):
        with Queue() as q:
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(os.path.relpath(q.path / "s"))
            listener.listen(1)
            try:
                p = q.popen("run", PY, WORKER, "event", "never")
                connection, _ = listener.accept()
                with connection:
                    self.assertEqual(receive_message(connection)[0], TYPES["GET_VERSION"])
                    send_message(connection, "VERSION", PROTOCOL)
                    self.assertEqual(receive_message(connection)[0], TYPES["GET_MAX_SLOTS"])
                    send_message(connection, "GET_MAX_SLOTS_OK", 4)
                    new = receive_message(connection)
                    self.assertEqual(new[0], TYPES["NEWJOB"])
                    self.assertEqual(connection.recv(8, socket.MSG_WAITALL), struct.pack("=Q", 0))
                    remaining = new[1] + new[4] + new[5]  # command, label, tag
                    while remaining:
                        remaining -= len(connection.recv(remaining))
                    send_message(connection, "NEWJOB_OK", 0)
                    send_message(connection, "RUNJOB", 0)
                    ready = receive_message(connection)
                    self.assertEqual(ready[0], TYPES["RUNJOB_OK"])
                    pgid = ready[3]
                    q.groups.add(pgid)
                    self.assertTrue(alive(pgid))
                    self.assertFalse((q.path / "never").exists())
                    p.kill()
                    self.assertEqual(p.wait(timeout=3), -signal.SIGKILL)
                    eventually(lambda: not alive(pgid))
                    self.assertFalse((q.path / "never").exists())
            finally:
                listener.close()

    def test_shutdown_skips_version_check(self):
        # An older server must still be stoppable after upgrading the client.
        with Queue() as q:
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(os.path.relpath(q.path / "s"))
            listener.listen(1)
            try:
                p = q.popen("shutdown")
                connection, _ = listener.accept()
                with connection:
                    self.assertEqual(receive_message(connection)[0], TYPES["KILL_SERVER"])
                self.assertEqual(p.communicate(timeout=3), (b"", b""))
                self.assertEqual(p.returncode, 0)
            finally:
                listener.close()

    def test_server_death_and_shutdown(self):
        for action in ("shutdown", signal.SIGTERM, signal.SIGKILL):
            with self.subTest(action=action), Queue(1) as q:
                p = q.start("running", tree=True)
                pids = q.pids("running")
                waiting = q.start("waiting", tree=True)
                q.wait_rows(2)
                server = q.server()
                if action == "shutdown":
                    self.assertEqual(q.call("shutdown"), (0, b"", b""))
                else:
                    os.kill(server, action)
                out, err = waiting.communicate(timeout=3)
                self.own_failure((waiting.returncode, out, err), "disconnected")
                self.assertFalse((q.path / "waiting.pids").exists())
                self.assertIsNone(p.poll())
                self.assertTrue(all(alive(pid) for pid in pids))
                os.killpg(pids[0], signal.SIGTERM)
                self.assertEqual(p.communicate(timeout=3), (b"", b""))
                self.assertEqual(p.returncode, 143)
                for pid in pids:
                    eventually(lambda pid=pid: not alive(pid))
                # Stale socket recovery after the old workload is stopped.
                self.assertEqual(q.call("run", "true"), (0, b"", b""))
                self.assertNotEqual(q.server(), server)

    def test_racing_first_clients_30_rounds(self):
        for trial in range(30):
            with self.subTest(trial=trial), Queue(1) as q:
                gate_r, gate_w = os.pipe()
                launcher = ("import os,sys; os.read(int(sys.argv[1]),1); "
                            "os.close(int(sys.argv[1])); os.execv(sys.argv[2],sys.argv[2:])")
                try:
                    children = []
                    for _ in range(2):
                        p = subprocess.Popen([PY, "-c", launcher, str(gate_r), BIN,
                                              "run", PY, WORKER, "race", "exclusive"],
                                             cwd=q.path, env=q.env, pass_fds=(gate_r,),
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        q.clients.append(p)
                        children.append(p)
                    os.close(gate_r)
                    gate_r = -1
                    os.write(gate_w, b"xx")
                    for p in children:
                        out, err = p.communicate(timeout=8)
                        self.assertEqual(out, b"")
                        self.assertTrue(all(line.startswith(b"junban: ") for line in err.splitlines()))
                        self.assertEqual(p.returncode, 0)
                    self.assertEqual(servers() - q.before_servers, {q.server()})
                finally:
                    os.close(gate_w)
                    if gate_r >= 0:
                        os.close(gate_r)

    def test_saturation(self):
        with Queue(1) as q:
            def limited_fds():
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))
            self.assertEqual(q.call("ls", preexec_fn=limited_fds)[0], 1)
            server = q.server()
            inode = (q.path / "s").stat().st_ino
            blocker = q.start("blocker", tree=True)
            pids = q.pids("blocker")
            waiters = []
            # With RLIMIT_NOFILE=64: 56 connections, 51 admitted jobs total.
            for i in range(50):
                waiters.append(q.popen("run", PY, WORKER, "event", "never-" + str(i)))
            self.assertEqual(len(q.wait_rows(51)), 51)
            self.own_failure(q.call("run", PY, WORKER, "event", "overflow"), "queue full")
            self.assertFalse((q.path / "overflow").exists())
            # Exhaust connection capacity as well, without launching 1000 processes.
            sockets = []
            try:
                for _ in range(12):
                    s = socket.socket(socket.AF_UNIX)
                    sockets.append(s)
                    s.connect(str(q.path / "s") if len(str(q.path / "s")) < 100
                              else self.relative_connect_path(q))
                self.own_failure(q.call("run", PY, WORKER, "event", "overflow2"), "queue full")
                self.assertEqual(servers() - q.before_servers, {server})
                self.assertEqual((q.path / "s").stat().st_ino, inode)
                self.assertFalse((q.path / "overflow2").exists())
            finally:
                for s in sockets:
                    s.close()
            for p in waiters:
                p.terminate()
                p.wait(timeout=3)
            eventually(lambda: len(q.rows()) == 1)
            self.assertEqual(q.call("kill", q.rows()[0][0]), (0, b"", b""))
            self.assertEqual(blocker.wait(timeout=3), 143)
            for pid in pids:
                eventually(lambda pid=pid: not alive(pid))
            self.assertFalse(list(q.path.glob("never-*")))

    def test_listen_backlog_saturation(self):
        with Queue(1) as q:
            p = q.start("blocker", tree=True)
            pids = q.pids("blocker")
            server = q.server()
            inode = (q.path / "s").stat().st_ino
            sockets = []
            os.kill(server, signal.SIGSTOP)
            try:
                full = False
                for _ in range(300):
                    s = socket.socket(socket.AF_UNIX)
                    s.setblocking(False)
                    sockets.append(s)
                    try:
                        s.connect(os.path.relpath(q.path / "s"))
                    except OSError as e:
                        self.assertIn(e.errno, (errno.ECONNREFUSED, errno.EAGAIN,
                                               errno.EINPROGRESS))
                        full = True
                        break
                self.assertTrue(full, "listen backlog was not filled")
                self.own_failure(q.call("run", PY, WORKER, "event", "never"), "queue full")
                self.assertEqual((q.path / "s").stat().st_ino, inode)
                self.assertEqual(servers() - q.before_servers, {server})
                self.assertFalse((q.path / "never").exists())
            finally:
                for s in sockets:
                    s.close()
                os.kill(server, signal.SIGCONT)
            self.assertEqual(q.call("kill", q.rows()[0][0]), (0, b"", b""))
            self.assertEqual(p.wait(timeout=3), 143)
            for pid in pids:
                eventually(lambda pid=pid: not alive(pid))

    @staticmethod
    def relative_connect_path(q):
        # A relative path from the runner's cwd avoids long sun_path values.
        return os.path.relpath(q.path / "s")


def artifact(name, data):
    destination = os.getenv("JUNBAN_TEST_ARTIFACTS")
    if destination:
        directory = Path(destination)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_bytes(data)


def read_until(stream, condition, timeout=5):
    data = b""
    descriptor = stream if isinstance(stream, int) else stream.fileno()
    end = time.monotonic() + timeout
    while not condition(data) and time.monotonic() < end:
        if select.select([stream], [], [], 0.1)[0]:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            data += chunk
    if not condition(data):
        raise AssertionError("expected output did not arrive: %r" % data)
    return data


def caffeinates(server):
    result = subprocess.run(["ps", "-axo", "pid=,ppid=,args="],
                            capture_output=True, text=True, check=True)
    found = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 2)
        if len(fields) == 3 and fields[1] == str(server) and "caffeinate -i -w" in fields[2]:
            found.append(int(fields[0]))
    return found


def terminal_modes(fd):
    modes = termios.tcgetattr(fd)
    # PENDIN describes pending kernel input reprocessing, not configuration.
    modes[3] &= ~getattr(termios, "PENDIN", 0)
    return modes


class VisibilityOnlyTests(unittest.TestCase):
    own_failure = QueueTests.own_failure

    def listing(self, q, all=False):
        code, out, err = q.call("ls", *(["-a"] if all else []))
        self.assertEqual((code, err), (0 if out else 1, b""))
        self.assertNotIn(b"junban:", out)
        self.assertNotIn(b"\x1b", out)
        return out.decode().splitlines()

    def test_output_stream_rules(self):
        with Queue() as q:
            for name in ("help", "--help", "version", "--version"):
                code, out, err = q.call(name)
                self.assertEqual((code, err), (0, b""))
                self.assertTrue(out)
                self.assertNotIn(b"junban:", out)
            self.assertEqual(q.call("ls"), (1, b"", b""))
            self.assertEqual(q.call("ls", "-a"), (1, b"", b""))
            # Agents read --help first: it must say how ls lines look and how to wait.
            text = q.call("help")[1].decode()
            for phrase in ("stdout has one line per job and nothing else",
                           "STATE id=N dir=DIR [tag=TAG] cpus=C gpus=G",
                           "junban wait ID...", "Do not loop over the output of junban ls",
                           "ls     0 if it listed a job, 1 if it listed none"):
                self.assertIn(phrase, text)
            self.assertLessEqual(max(map(len, text.splitlines())), 100)
            self.assertEqual(q.call("version")[1], b"junban 0.1.1 (based on task-spooler 1.0.4)\n")
            self.assertEqual(q.call("ls", "--summary"), (0, b"cpus 0/4 gpus 0/1 running 0 queued 0\n", b""))
            for args in (["--summary", "-a"], ["-a", "-a"], ["--summary", "--summary"]):
                self.own_failure(q.call("ls", *args))
            self.assertEqual(q.call("run", "sh", "-c", "printf OUT; printf ERR >&2"),
                             (0, b"OUT", b"ERR"))
            self.own_failure(q.call("ls", "--bad"))

    def test_ls_exit_status(self):
        with Queue(2) as q:
            self.assertEqual(q.call("ls"), (1, b"", b""))
            p = q.start("job", 1)
            q.pids("job")
            code, out, err = q.call("ls")
            self.assertEqual((code, err), (0, b""))
            self.assertTrue(out.startswith(b"running id=0 "))
            q.release("job")
            self.assertEqual(p.wait(timeout=3), 0)
            eventually(lambda: q.call("ls")[0] == 1)
            self.assertEqual(q.call("ls", "-a")[0], 0)
            env = dict(q.env, JUNBAN_SOCKET="missing/s")
            for args in (["ls"], ["ls", "-a"], ["ls", "--summary"]):
                self.own_failure(q.call(*args, env=env))

    def test_ls_filters(self):
        with Queue(4) as q:
            q.call("ls")
            for name in ("alpha", "beta", "sp ace"):
                (q.path / name).mkdir()
            env = dict(q.env, JUNBAN_SOCKET="../s")
            self.assertEqual(q.call("run", "true", cwd=q.path / "alpha", env=env), (0, b"", b""))
            a = q.popen("run", "-c", "1", PY, WORKER, "hold", q.path / "a.pids",
                        q.path / "a.release", cwd=q.path / "alpha", env=env)
            q.pids("a")
            b = q.popen("run", "-c", "1", PY, WORKER, "hold", q.path / "b.pids",
                        q.path / "b.release", cwd=q.path / "beta", env=env)
            q.pids("b")
            c = q.popen("run", "-c", "1", PY, WORKER, "hold", q.path / "c.pids",
                        q.path / "c.release", cwd=q.path / "sp ace", env=env)
            q.pids("c")
            def listed(*args):
                code, out, err = q.call("ls", *args)
                self.assertEqual((code, err), (0 if out else 1, b""), args)
                return [int(line.split()[1][3:]) for line in out.decode().splitlines()]
            self.assertEqual(listed(), [1, 2, 3])
            self.assertEqual(listed("--id", "2"), [2])
            self.assertEqual(listed("--id", "3", "--id", "1"), [1, 3])
            self.assertEqual(listed("--id", "0"), [])
            self.assertEqual(listed("--id", "0", "-a"), [0])
            self.assertEqual(listed("--id", "99"), [])
            self.assertEqual(listed("--dir", "alpha"), [1])
            self.assertEqual(listed("--dir", "alpha", "-a"), [1, 0])
            self.assertEqual(listed("--dir", "alpha", "--dir", "beta"), [1, 2])
            self.assertEqual(listed("--dir", "sp%20ace"), [3])
            self.assertEqual(listed("--dir", "sp ace"), [3])
            self.assertEqual(listed("--dir", "alpha", "--id", "2"), [])
            self.assertEqual(listed("--dir", "beta", "--id", "2"), [2])
            self.assertEqual(listed("-a", "--dir", "alpha", "--id", "0"), [0])
            self.assertEqual(q.call("ls", "--id", "2")[0], 0)
            self.assertEqual(q.call("ls", "--id", "99")[0], 1)
            for args in (["--id"], ["--id", "x"], ["--id", "-1"], ["--dir"], ["--dir", ""],
                         ["--summary", "--id", "1"], ["--summary", "--dir", "alpha"]):
                self.own_failure(q.call("ls", *args))
            for name, p in (("a", a), ("b", b), ("c", c)):
                q.release(name)
                self.assertEqual(p.wait(timeout=3), 0)

    def test_tags(self):
        with Queue(4) as q:
            def listed(*args):
                code, out, err = q.call("ls", *args)
                self.assertEqual((code, err), (0 if out else 1, b""), args)
                return [(int(line.split()[1][3:]), re.search(r" tag=(\S+)", line) and
                         re.search(r" tag=(\S+)", line).group(1))
                        for line in out.decode().splitlines()]
            def hold(name, *opts, env=None):
                return q.popen("run", "-c", "1", *opts, PY, WORKER, "hold", name + ".pids",
                               name + ".release", env=env or q.env)
            agent = dict(q.env, JUNBAN_TAG="agent-1")
            a = hold("a", env=agent)
            q.pids("a")
            b = hold("b", "--tag", "other", env=agent)
            q.pids("b")
            c = hold("c")
            q.pids("c")
            d = hold("d", "--tag", "agent-1")
            q.pids("d")
            e = q.popen("run", "--tag", "agent-1", PY, WORKER, "hold", "e.pids", "e.release")
            eventually(lambda: len(listed()) == 5)
            self.assertEqual(listed(), [(0, "agent-1"), (1, "other"), (2, None),
                                        (3, "agent-1"), (4, "agent-1")])
            self.assertRegex(q.call("ls", "--id", "0")[1].decode(),
                             r"^running id=0 dir=\S+ tag=agent-1 cpus=1 gpus=0 elapsed=\d+s cmd=")
            self.assertEqual(listed("--tag", "agent-1"), [(0, "agent-1"), (3, "agent-1"), (4, "agent-1")])
            self.assertEqual(listed("--tag", "agent-1", "--tag", "other"),
                             [(0, "agent-1"), (1, "other"), (3, "agent-1"), (4, "agent-1")])
            self.assertEqual(listed("--tag", "agent-1", "--id", "1"), [])
            self.assertEqual(listed("--tag", "nobody"), [])
            # Killing by tag stops running jobs and removes queued ones, only for that tag.
            self.assertEqual(q.call("kill", "--tag", "agent-1"), (0, b"", b""))
            self.assertEqual(a.wait(timeout=3), 143)
            self.assertEqual(d.wait(timeout=3), 143)
            out, err = e.communicate(timeout=3)
            self.own_failure((e.returncode, out, err), "removed")
            eventually(lambda: listed() == [(1, "other"), (2, None)])
            self.assertEqual(sorted(listed("-a", "--tag", "agent-1")),
                             [(0, "agent-1"), (3, "agent-1"), (4, "agent-1")])
            self.assertEqual(q.call("kill", "--tag", "agent-1"),
                             (1, b"", b"junban: no running or queued job with tag agent-1\n"))
            for args in (["run", "--tag", "", "true"], ["run", "--tag", "a b", "true"],
                         ["run", "--tag", "x" * 64, "true"], ["run", "--tag"],
                         ["run", "--tag", "a", "--tag", "b", "true"], ["ls", "--tag"],
                         ["ls", "--tag", "a/b"], ["ls", "--summary", "--tag", "a"],
                         ["kill", "--tag"], ["kill", "--tag", "a", "b"], ["kill", "--tag", "%"]):
                self.own_failure(q.call(*args))
            self.own_failure(q.call("run", "true", env=dict(q.env, JUNBAN_TAG="bad tag")), "JUNBAN_TAG")
            self.assertEqual(q.call("run", "-c", "1", "true", env=dict(q.env, JUNBAN_TAG="")), (0, b"", b""))
            self.assertEqual(q.call("run", "-c", "1", "--tag", "x" * 63, "true", env=agent), (0, b"", b""))
            self.assertEqual(listed("-a", "--tag", "x" * 63)[0][1], "x" * 63)
            self.assertEqual(listed("-a", "--id", "5"), [(5, None)])
            for name, p in (("b", b), ("c", c)):
                q.release(name)
                self.assertEqual(p.wait(timeout=3), 0)

    def test_detach(self):
        with Queue(2) as q:
            blocker = q.start("blocker", 1)
            q.pids("blocker")
            started = time.monotonic()
            p = q.popen("run", "--detach", "-c", "1", "--tag", "d", PY, "-c",
                        "import sys, time; print('OUT', flush=True); "
                        "print('ERR', file=sys.stderr, flush=True); "
                        "time.sleep(0.5); sys.exit(3)")
            # The caller's pipes reach EOF at once although the job runs on.
            out, err = p.communicate(timeout=3)
            self.assertLess(time.monotonic() - started, 2)
            self.assertEqual((p.returncode, out), (0, b"1\n"))
            log = (q.path / "s.logs" / "1.log").resolve()
            self.assertEqual(err.decode(), "junban: id=1 log=%s\n" % log)
            self.assertEqual(log.stat().st_mode & 0o777, 0o600)
            self.assertEqual((q.path / "s.logs").stat().st_mode & 0o777, 0o700)
            line = eventually(lambda: q.call("ls", "-a", "--id", "1")[1].decode())
            self.assertIn(" tag=d ", line)
            self.assertIn(" log=%s cmd=" % log, line)
            eventually(lambda: "exit=3" in q.call("ls", "-a", "--id", "1")[1].decode())
            self.assertEqual(log.read_text(), "OUT\nERR\n")
            # A detached job waits for its turn like any other job, silently.
            p = q.popen("run", "--detach", PY, WORKER, "hold", "queued.pids", "queued.release")
            out, err = p.communicate(timeout=3)
            self.assertEqual((p.returncode, out), (0, b"2\n"))
            self.assertEqual(q.rows(), [(0, "running"), (2, "queued")])
            q.release("blocker")
            self.assertEqual(blocker.wait(timeout=3), 0)
            q.pids("queued")
            self.assertEqual(q.rows(), [(2, "running")])
            q.release("queued")
            eventually(lambda: "exit=0" in q.call("ls", "-a", "--id", "2")[1].decode())
            self.assertEqual((q.path / "s.logs" / "2.log").read_bytes(), b"")
            # Killing a queued detached job removes it.
            blocker = q.start("blocker2", 1)
            q.pids("blocker2")
            self.assertEqual(q.call("run", "--detach", "true")[:2], (0, b"4\n"))
            self.assertEqual(q.call("kill", "4"), (0, b"", b""))
            eventually(lambda: " killed " in q.call("ls", "-a", "--id", "4")[1].decode())
            q.release("blocker2")
            self.assertEqual(blocker.wait(timeout=3), 0)
            # Errors before queuing still go to the caller.
            self.own_failure(q.call("run", "--detach", "--detach", "true"))
            self.own_failure(q.call("run", "--detach"))
            self.own_failure(q.call("run", "--detach", "-c", "3", "true"), "CPUs")
            (q.path / "sub").mkdir()
            env = dict(q.env, JUNBAN_SOCKET="../s")
            code, out, err = q.call("run", "--detach", "-c", "1", "pwd", cwd=q.path / "sub", env=env)
            self.assertEqual((code, out), (0, b"5\n"))
            eventually(lambda: "exit=0" in q.call("ls", "-a", "--id", "5")[1].decode())
            self.assertEqual((q.path / "s.logs" / "5.log").read_text().strip(),
                             str((q.path / "sub").resolve()))

    def test_wait(self):
        with Queue(2) as q:
            def detach(*args):
                code, out, err = q.call("run", "--detach", *args)
                self.assertEqual(code, 0, err)
                return out.decode().strip()
            def hold(name, *opts):
                return detach("-c", "1", *opts, PY, WORKER, "hold", name + ".pids", name + ".release")
            done = detach("-c", "1", "sh", "-c", "exit 3")
            eventually(lambda: "exit=3" in q.call("ls", "-a", "--id", done)[1].decode())
            # Finished jobs are judged at once, with or without a timeout.
            self.assertEqual(q.call("wait", done), (3, b"", b""))
            self.assertEqual(q.call("wait", "--timeout", "1s", done), (3, b"", b""))
            code, out, err = q.call("wait", "-v", done)
            self.assertEqual((code, err), (3, b""))
            self.assertRegex(out.decode(), r"^finished id=0 exit=3 elapsed=\d+s\n$")
            # A running job and a queued job behind it.
            running = hold("running")
            q.pids("running")
            queued = detach("sh", "-c", "exit 5")
            self.assertEqual(q.rows(), [(1, "running"), (2, "queued")])
            for ids in ([running], [queued], [running, queued, done]):
                started = time.monotonic()
                self.assertEqual(q.call("wait", "--timeout", "1s", *ids),
                                 (124, b"", b"junban: timed out with %d of %d jobs unfinished\n"
                                  % (len(set(ids) - {done}), len(ids))))
                self.assertGreaterEqual(time.monotonic() - started, 0.9)
            waiter = q.popen("wait", "-v", running, queued, done)
            timed = q.popen("wait", "--timeout", "1h", queued)
            time.sleep(0.5)
            self.assertIsNone(waiter.poll())
            self.assertIsNone(timed.poll())
            q.release("running")
            out, err = waiter.communicate(timeout=5)
            self.assertEqual((waiter.returncode, err), (5, b""))
            lines = out.decode().splitlines()
            self.assertEqual(sorted(line.split()[1] for line in lines), ["id=0", "id=1", "id=2"])
            self.assertRegex(lines[-1], r"^finished id=2 exit=5 elapsed=\d+s$")
            self.assertEqual(timed.communicate(timeout=5), (b"", b""))
            self.assertEqual(timed.returncode, 5)
            self.assertEqual(q.call("wait", running), (0, b"", b""))
            # Tags: all running and queued jobs with the tag, also ones queued later.
            a = hold("a", "--tag", "t")
            q.pids("a")
            b = hold("b", "--tag", "t")
            q.pids("b")
            other = hold("other", "--tag", "u")
            tagged = q.popen("wait", "-v", "--tag", "t")
            time.sleep(0.3)
            later = detach("-c", "1", "--tag", "t", "sh", "-c", "exit 7")
            q.release("a")
            q.release("b")
            out, err = tagged.communicate(timeout=5)
            self.assertEqual((tagged.returncode, err), (7, b""), out)
            self.assertEqual(sorted(line.split()[1] for line in out.decode().splitlines()),
                             sorted("id=" + i for i in (a, b, later)))
            self.assertEqual(q.call("wait", "--tag", "t"), (0, b"", b""))
            self.assertEqual(q.call("wait", "--timeout", "1s", "--tag", "u", "--tag", "t")[0], 124)
            q.pids("other")
            q.release("other")
            self.assertEqual(q.call("wait", "--tag", "u", done), (3, b"", b""))
            # Killed jobs report their status; killed queued jobs 125.
            blocker = hold("blocker")
            q.pids("blocker")
            removed = detach("true")
            self.assertEqual(q.call("kill", removed), (0, b"", b""))
            self.assertEqual(q.call("kill", blocker), (0, b"", b""))
            code, out, err = q.call("wait", "-v", blocker, removed)
            self.assertEqual((code, err), (143, b""))
            self.assertIn(b"finished id=%s killed " % removed.encode(), out)
            # Results stay available after they drop out of ls -a.
            for _ in range(25):
                detach("-c", "1", "true")
            eventually(lambda: q.call("ls")[0] == 1)
            self.assertEqual(q.call("ls", "-a", "--id", done)[0], 1)
            self.assertEqual(q.call("wait", done), (3, b"", b""))
            # junban errors.
            self.own_failure(q.call("wait", "9999"), "unknown job ID 9999")
            self.own_failure(q.call("wait", done, "9999"), "unknown")
            for args in ([], ["-v"], ["--timeout", "1s"], ["x"], ["-1"], ["--timeout"],
                         ["--timeout", "0s", "0"], ["--timeout", "1", "0"], ["--tag"],
                         ["--tag", "a b"], ["-v", "-v", "0"], ["--timeout", "1s", "--timeout", "1s", "0"]):
                self.own_failure(q.call("wait", *args))
            env = dict(q.env, JUNBAN_SOCKET="missing/s")
            self.own_failure(q.call("wait", "0", env=env))
            code, out, err = q.call("run", "-c", "1", "sh", "-c", BIN + ' wait "$JUNBAN_JOB_ID"')
            self.assertEqual(code, 125)
            self.assertIn(b"cannot wait for itself", err)

    def test_review_fixes_wait_and_detach(self):
        with Queue(2) as q:
            # A tagged job running junban wait --tag on its own tag is refused.
            env = dict(q.env, JUNBAN_TAG="self")
            code, out, err = q.call("run", "-c", "1", "sh", "-c", BIN + " wait --tag self", env=env)
            self.assertEqual(code, 125)
            self.assertIn(b"cannot wait for itself", err)
            # JUNBAN_TAG is only a default: a valid --tag overrides a bad one.
            env = dict(q.env, JUNBAN_TAG="bad tag")
            self.assertEqual(q.call("run", "-c", "1", "--tag", "good", "true", env=env), (0, b"", b""))
            self.assertIn(b" tag=good ", q.call("ls", "-a", "--tag", "good")[1])
            # A detached job holds no inherited descriptor, however high.
            reader, writer = os.pipe()
            def high_fd():
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                limit = 6000 if hard == resource.RLIM_INFINITY else min(hard, 6000)
                resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
                os.dup2(writer, limit - 1)
            try:
                p = q.popen("run", "--detach", "-c", "1", PY, WORKER, "hold", "high.pids",
                            "high.release", preexec_fn=high_fd, close_fds=False)
                os.close(writer)
                writer = -1
                self.assertEqual(p.wait(timeout=3), 0)
                q.pids("high")
                self.assertTrue(select.select([reader], [], [], 3)[0])
                self.assertEqual(os.read(reader, 1), b"")
            finally:
                os.close(reader)
                if writer >= 0:
                    os.close(writer)
            q.release("high")

    def test_wait_timeout_covers_stalled_server(self):
        with Queue() as q:
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(os.path.relpath(q.path / "s"))
            listener.listen(1)
            try:
                started = time.monotonic()
                p = q.popen("wait", "--timeout", "1s", "0")
                connection, _ = listener.accept()
                with connection:
                    self.assertEqual(receive_message(connection)[0], TYPES["GET_VERSION"])
                    send_message(connection, "VERSION", PROTOCOL)
                    self.assertEqual(receive_message(connection)[0], TYPES["JOB_INFO"])
                    out, err = p.communicate(timeout=5)
                self.assertEqual((p.returncode, out), (124, b""))
                self.assertIn(b"timed out", err)
                self.assertLess(time.monotonic() - started, 3)
            finally:
                listener.close()

    def test_review_fixes_logs_kill_and_handshake(self):
        with Queue(2) as q:
            # A planted symlink in the log directory is not followed.
            q.call("ls")
            (q.path / "s.logs").mkdir(mode=0o755)
            victim = q.path / "victim"
            victim.write_text("keep")
            (q.path / "s.logs" / "0.log").symlink_to(victim)
            code, out, err = q.call("run", "--detach", "-c", "1", "true")
            self.assertEqual(code, 125, err)
            self.assertIn(b"cannot open job log", err)
            self.assertEqual(victim.read_text(), "keep")
            self.assertEqual((q.path / "s.logs").stat().st_mode & 0o777, 0o700)
            (q.path / "s.logs" / "0.log").unlink()
            (q.path / "s.logs").rmdir()
            (q.path / "s.logs").symlink_to(q.path)
            self.own_failure(q.call("run", "--detach", "-c", "1", "true"), "owned by this user")
            (q.path / "s.logs").unlink()
            # With stdin closed the queue connection survives detaching.
            code, out, err = q.call("run", "--detach", "-c", "1", "true",
                                    stdin=None, preexec_fn=lambda: os.close(0))
            self.assertEqual(code, 0, err)
            job = out.decode().strip()
            eventually(lambda: " exit=0 " in q.call("ls", "-a", "--id", job)[1].decode())
            # kill --tag from inside a tagged job stops the other jobs too.
            killer = q.popen("run", "-c", "1", "--tag", "t", "sh", "-c",
                             "while [ ! -f other.pids ]; do sleep 0.02; done; "
                             + BIN + " kill --tag t; sleep 5")
            other = q.popen("run", "-c", "1", "--tag", "t", PY, WORKER, "hold",
                            "other.pids", "other.release")
            q.pids("other")
            self.assertEqual(other.wait(timeout=5), 143)
            self.assertEqual(killer.wait(timeout=5), 143)

    def test_detach_logs_per_socket_and_closed_streams(self):
        with Queue(2) as q:
            # Two queues whose sockets share a directory keep separate logs.
            for name in ("a", "b"):
                env = dict(q.env, JUNBAN_SOCKET=name)
                code, out, err = q.call("run", "--detach", "-c", "1", "echo", name, env=env)
                self.assertEqual((code, out), (0, b"0\n"), err)
            for name in ("a", "b"):
                log = q.path / (name + ".logs") / "0.log"
                eventually(lambda log=log: log.exists() and log.read_text() == name + "\n")
            for name in ("a", "b"):
                self.assertEqual(q.call("shutdown", env=dict(q.env, JUNBAN_SOCKET=name))[0], 0)
            # A closed stdout or stderr of the caller never becomes the job log.
            for fd, job in ((2, 0), (1, 1)):
                code, out, err = q.call("run", "--detach", "-c", "1", "echo", "job",
                                        preexec_fn=lambda fd=fd: os.close(fd),
                                        **({"stdout": None} if fd == 1 else {"stderr": None}))
                self.assertEqual(code, 0 if fd == 2 else 125, (fd, out, err))
                if fd == 2:
                    self.assertEqual(out, b"0\n")
                log = q.path / "s.logs" / ("%d.log" % job)
                eventually(lambda log=log: log.exists() and log.read_text() == "job\n")

    def test_wait_timeout_covers_handshake(self):
        with Queue() as q:
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(os.path.relpath(q.path / "s"))
            listener.listen(1)
            try:
                started = time.monotonic()
                p = q.popen("wait", "--timeout", "1s", "0")
                connection, _ = listener.accept()
                with connection:
                    self.assertEqual(receive_message(connection)[0], TYPES["GET_VERSION"])
                    out, err = p.communicate(timeout=5)
                self.assertEqual((p.returncode, out), (124, b""))
                self.assertIn(b"timed out", err)
                self.assertLess(time.monotonic() - started, 3)
            finally:
                listener.close()

    def test_estimate_parsing_and_units(self):
        with Queue() as q:
            for value in ("", "1", "s", "0s", "-1m", "+1s", "1.5h", "1ms", "1w",
                          "1sX", " 1s", "18446744073709551616s", "18446744073709551615d"):
                self.own_failure(q.call("run", "--estimate", value, "true"))
            self.own_failure(q.call("run", "--estimate"))
            self.own_failure(q.call("run", "--estimate", "1s", "--estimate", "1m", "true"))
            self.assertFalse((q.path / "s").exists())
            jobs = []
            for i, duration in enumerate(("90s", "30m", "2h", "1d")):
                jobs.append(q.start(str(i), 1, estimate=duration))
                q.pids(str(i))
            lines = self.listing(q)
            for expected in ("elapsed=0s est=1m30s", "elapsed=0s est=30m", "elapsed=0s est=2h", "elapsed=0s est=1d"):
                self.assertTrue(any(expected in line for line in lines), lines)
            for i, p in enumerate(jobs):
                q.release(str(i))
                self.assertEqual(p.wait(timeout=3), 0)

    def listing_ids(self, q, *args):
        code, out, err = q.call("ls", "-a", *args)
        self.assertEqual((code, err), (0 if out else 1, b""))
        return [int(line.split()[1][3:]) for line in out.decode().splitlines()]

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux allows invalid UTF-8 names")
    def test_cwd_label_invalid_utf8(self):
        with Queue() as q:
            q.call("ls")
            directory = os.path.join(os.fsencode(q.path), b"bad\xff\xe6\x97")
            os.mkdir(directory)
            env = dict(q.env, JUNBAN_SOCKET="../s")
            self.assertEqual(q.call("run", "true", cwd=directory, env=env), (0, b"", b""))
            out = eventually(lambda: q.call("ls", "-a")[1])
            self.assertIn(b" dir=bad%FF%E6%97 ", out)

    def test_watch_wide_directory_names(self):
        for name, shown in (("日本語のとても長いディレクトリ", "日本語のとても長"),
                            ("🚀" * 10, "🚀" * 8)):
            with self.subTest(name=name):
                self.watch_wide_directory(name, shown)

    def watch_wide_directory(self, name, shown):
        with Queue() as q:
            q.call("ls")
            directory = q.path / name
            directory.mkdir()
            env = dict(q.env, JUNBAN_SOCKET="../s")
            job = q.popen("run", "-c", "1", PY, WORKER, "hold", q.path / "wide.pids",
                          q.path / "wide.release", cwd=directory, env=env)
            q.pids("wide")
            master, slave = pty.openpty()
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 60, 0, 0))
            def controlling_terminal():
                os.setsid()
                fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                os.tcsetpgrp(0, os.getpgrp())
            try:
                p = subprocess.Popen([BIN, "watch"], cwd=q.path, env=q.env, stdin=slave,
                                     stdout=slave, stderr=slave, preexec_fn=controlling_terminal)
                q.clients.append(p)
                screen = read_until(master, lambda data: shown[:2].encode() in data)
                p.terminate()
                end = time.monotonic() + 3
                while p.poll() is None and time.monotonic() < end:
                    if select.select([master], [], [], 0.05)[0]:
                        os.read(master, 65536)
                self.assertEqual(p.wait(timeout=1), 0)
            finally:
                os.close(master)
                os.close(slave)
            clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", screen).decode().replace("\r", "")
            def columns(text):
                return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)
            rows = clean.split("RUNNING", 1)[1].splitlines()
            header, row = rows[1], rows[2]
            self.assertNotIn("%E", row)
            # The DIR cell holds whole characters within 16 columns, so the
            # CPUS column lines up with its header.
            self.assertIn(" %s " % shown, row)
            self.assertEqual(columns(header[:header.index("CPUS") + 4]),
                             columns(row[:row.index(" 1 ") + 2]))
            self.assertTrue(all(columns(line) < 60 for line in clean.splitlines()), clean)
            q.release("wide")
            self.assertEqual(job.wait(timeout=3), 0)

    def test_cwd_label(self):
        with Queue() as q:
            q.call("ls")
            names = ("AHC075-a", "kaggle-x", "space\n\x1b-label", "課題",
                     "テ\u3099ータ", "c1\u0085-%", "長" * 50, "a\u3000b", "l\u2028s",
                     "bidi\u202e", "🚀")
            for name in names:
                directory = q.path / name
                directory.mkdir()
                env = dict(q.env, JUNBAN_SOCKET="../s")
                self.assertEqual(q.call("run", "true", cwd=directory, env=env), (0, b"", b""))
            lines = eventually(lambda: self.listing(q, True) if len(self.listing(q, True)) == len(names) else None)
            self.assertTrue(any(" dir=AHC075-a " in line for line in lines))
            self.assertTrue(any(" dir=kaggle-x " in line for line in lines))
            self.assertTrue(any(" dir=space%0A%1B-label " in line for line in lines))
            # Printable UTF-8 stays readable; C1 controls, spaces and % are encoded.
            self.assertTrue(any(" dir=課題 " in line for line in lines), lines)
            self.assertTrue(any(" dir=テ\u3099ータ " in line for line in lines), lines)
            self.assertTrue(any(" dir=c1%C2%85-%25 " in line for line in lines), lines)
            # Unicode whitespace, line separators and format characters too.
            for encoded in ("a%E3%80%80b", "l%E2%80%A8s", "bidi%E2%80%AE", "🚀"):
                self.assertTrue(any(" dir=%s " % encoded in line for line in lines), (encoded, lines))
            # Long names are cut on a character boundary.
            self.assertTrue(any(re.search(r" dir=長{40,42}\.\.\. ", line) for line in lines), lines)
            self.assertEqual(self.listing_ids(q, "--dir", "課題"), [3])
            self.assertNotIn("\x1b", "\n".join(lines))
            self.assertNotIn("\u0085", "\n".join(lines))

    def test_history_last_20_newest_first(self):
        with Queue() as q:
            for i in range(25):
                self.assertEqual(q.call("run", "sh", "-c", "exit %d" % (i % 8)),
                                 (i % 8, b"", b""))
            eventually(lambda: len(self.listing(q, True)) == 20)
            short, full = self.listing(q), self.listing(q, True)
            self.assertEqual(short, [])
            self.assertEqual([int(line.split()[1].split("=")[1]) for line in full], list(range(24, 4, -1)))
            for i, line in zip(range(24, 4, -1), full):
                self.assertRegex(line, r"^done id=%d dir=\S+ cpus=all\(4\) gpus=0 elapsed=\d+s exit=%d cmd=sh -c exit %d$"
                                 % (i, i % 8, i % 8))
            self.own_failure(q.call("kill", "24"), "unknown")
            self.own_failure(q.call("kill", "9999"), "unknown")
            self.assertEqual(self.listing(q, True), full)

    def test_listing_states_over_and_killed(self):
        with Queue(4) as q:
            p = q.start("running", 3, estimate="1s")
            pids = q.pids("running")
            waiting = q.start("waiting", estimate="2m")
            q.wait_rows(2)
            later = q.start("later", 1)
            q.wait_rows(3)
            lines = self.listing(q)
            self.assertEqual(q.call("ls", "--summary"), (0, b"cpus 3/4 gpus 0/1 running 1 queued 2\n", b""))
            self.assertEqual(len(lines), 3)
            self.assertRegex(lines[0], r"^running id=0 dir=\S+ cpus=3 gpus=0 elapsed=\d+s est=1s(?: over)? cmd=.+")
            self.assertRegex(lines[1], r"^queued id=1 dir=\S+ cpus=all\(4\) gpus=0 waited=\d+s est=2m cmd=.+")
            self.assertRegex(lines[2], r"^queued id=2 dir=\S+ cpus=1 gpus=0 waited=\d+s cmd=.+")
            self.assertLessEqual(len(lines[0].split(" cmd=", 1)[1]), 80)
            eventually(lambda: " over " in self.listing(q)[0])
            self.assertIsNone(p.poll(), "estimate must never stop the job")
            artifact("ls-over.txt", ("\n".join(self.listing(q)) + "\n").encode())
            self.assertEqual(q.call("kill", "1"), (0, b"", b""))
            out, err = waiting.communicate(timeout=3)
            self.own_failure((waiting.returncode, out, err), "removed")
            q.pids("later")
            self.assertTrue(any(re.match(r"done id=1 dir=\S+ cpus=all\(4\) gpus=0 elapsed=0s est=2m killed cmd=", line)
                                for line in self.listing(q, True)))
            self.assertEqual(q.call("kill", "0"), (0, b"", b""))
            self.assertEqual(p.wait(timeout=3), 143)
            for pid in pids:
                eventually(lambda pid=pid: not alive(pid))
            q.release("later")
            self.assertEqual(later.wait(timeout=3), 0)
            self.assertEqual(q.call("run", "sh", "-c", "kill -TERM $$"), (143, b"", b""))
            eventually(lambda: any("signal=15" in line for line in self.listing(q, True)))
            artifact("ls-finished.txt", ("\n".join(self.listing(q, True)) + "\n").encode())

    def test_wait_notifications(self):
        with Queue(4) as q:
            self.assertEqual(q.call("run", "true"), (0, b"", b""))
            a = q.start("a", 1)
            q.pids("a")
            b = q.start("b", 1, estimate="30m")
            q.pids("b")
            env = dict(q.env, JUNBAN_NOTIFY_SEC="0.2")
            waiting = q.start("waiting", env=env)
            output = read_until(waiting.stderr, lambda data: len(data.splitlines()) >= 6)
            lines = output.splitlines()
            self.assertIn(b"id=3 queued, cpus=all(4), gpus=0, 2 ahead, waiting for cpus", lines[0])
            self.assertRegex(lines[1], rb"^junban:   running id=1 dir=\S+ cpus=1 gpus=0 elapsed=\d+s$")
            self.assertRegex(lines[2], rb"^junban:   running id=2 dir=\S+ cpus=1 gpus=0 elapsed=\d+s est=30m$")
            self.assertEqual(lines[0], lines[3])
            q.release("a")
            q.release("b")
            self.assertEqual(a.wait(timeout=3), 0)
            self.assertEqual(b.wait(timeout=3), 0)
            output += read_until(waiting.stderr, lambda data: b"started after" in data)
            q.pids("waiting")
            q.release("waiting")
            out, err = waiting.communicate(timeout=3)
            output += err
            self.assertEqual((waiting.returncode, out), (0, b""))
            self.assertRegex(output.splitlines()[-1], rb"^junban: id=3 started after \d+s$")
            self.assertEqual(output.count(b"started after"), 1)
            artifact("run-stderr.txt", output)

    def test_watch_non_tty(self):
        with Queue() as q:
            self.own_failure(q.call("watch"), "use junban ls")
            self.assertFalse((q.path / "s").exists())

    def test_watch_pty_modes_and_resize(self):
        with Queue() as q:
            for i in range(7):
                self.assertEqual(q.call("run", "sh", "-c", "exit %d" % i)[0], i)
            job = q.start("watch-job", 3, estimate="1s")
            q.pids("watch-job")
            waiting = q.start("watch-wait")
            q.wait_rows(2)
            eventually(lambda: " over " in self.listing(q)[0])
            for method in ("q", "ctrl-c"):
                master, slave = pty.openpty()
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
                before = terminal_modes(slave)
                def controlling_terminal():
                    os.setsid()
                    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                    os.tcsetpgrp(0, os.getpgrp())
                try:
                    p = subprocess.Popen([PY, WORKER, "watch-host", BIN],
                                         cwd=q.path, env=q.env,
                                         stdin=slave, stdout=slave, stderr=slave,
                                         preexec_fn=controlling_terminal)
                    q.clients.append(p)
                    screen = read_until(master, lambda data: b"exit=2" in data)
                    self.assertIn(b"\x1b[?1049h", screen)
                    self.assertIn(b"\x1b[?25l", screen)
                    self.assertIn(b"\x1b[1;31m", screen)
                    self.assertIn(b"cpus 3/4 [", screen)
                    self.assertIn(b"OVER", screen)
                    clean_screen = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", screen)
                    done_rows = clean_screen.split(b"DONE", 1)[1].splitlines()[2:]
                    self.assertEqual(len(done_rows), 5)
                    self.assertEqual([int(row.split()[0]) for row in done_rows], [6, 5, 4, 3, 2])
                    self.assertIn(b"RESULT", clean_screen)
                    self.assertNotIn(b"ST ID", clean_screen)
                    self.assertNotEqual(terminal_modes(slave), before)
                    if method == "q":
                        artifact("watch-ansi.txt", screen)
                        clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", screen)
                        artifact("watch-screen.txt", clean.replace(b"\r", b""))
                        self.assertTrue(all(len(line) < 80 for line in clean.splitlines()))
                        # The same screen data must refresh correctly, not accumulate rows.
                        refreshed = read_until(master, lambda data: b"exit=2" in data, 4)
                        self.assertEqual(refreshed.count(b"RUNNING"), 1)
                        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 20, 40, 0, 0))
                        narrow = read_until(master, lambda data: b"exit=2" in data, 4)
                        clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", narrow)
                        self.assertTrue(all(len(line) < 40 for line in clean.splitlines()), clean)
                        artifact("watch-40.txt", clean.replace(b"\r", b""))
                        for token in (b"DIR", b"CPUS", b"ELAPSED", b"WAITED", b"EST", b"RESULT", b"COMMAND", b"OVER", b"all", b"q:quit"):
                            self.assertIn(token, clean)
                        self.assertNotIn(b"ST ID", clean)
                        os.write(master, b"q")
                    else:
                        os.write(master, b"\x03")
                    # Drain terminal output while waiting for exit; a PTY can
                    # apply output backpressure, especially after a resize.
                    tail = b""
                    end = time.monotonic() + 3
                    while b"WATCH-EXIT:" not in tail and time.monotonic() < end:
                        if select.select([master], [], [], 0.05)[0]:
                            tail += os.read(master, 65536)
                    self.assertIn(b"WATCH-EXIT:0 MODES:restored", tail, (method, tail))
                    self.assertEqual(terminal_modes(slave), before)
                    while select.select([master], [], [], 0.05)[0]:
                        tail += os.read(master, 65536)
                    self.assertIn(b"\x1b[?25h", tail)
                    self.assertIn(b"\x1b[?1049l", tail)
                    p.terminate()
                    p.wait(timeout=3)
                finally:
                    if p.poll() is None:
                        p.terminate()
                        try:
                            p.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            p.kill()
                            p.wait(timeout=3)
                    os.close(master)
                    os.close(slave)
            self.assertEqual(q.call("kill", q.rows()[1][0]), (0, b"", b""))
            waiting.communicate(timeout=3)
            q.release("watch-job")
            self.assertEqual(job.wait(timeout=3), 0)

    @unittest.skipUnless(sys.platform == "darwin", "macOS caffeinate only")
    def test_sleep_prevention_active_idle(self):
        with Queue(1) as q:
            q.call("ls")
            server = q.server()
            self.assertEqual(caffeinates(server), [])
            running = q.start("running")
            q.pids("running")
            child = eventually(lambda: caffeinates(server))
            self.assertEqual(len(child), 1)
            waiting = q.start("waiting")
            q.wait_rows(2)
            self.assertEqual(caffeinates(server), child)
            q.release("running")
            self.assertEqual(running.wait(timeout=3), 0)
            q.pids("waiting")
            self.assertEqual(caffeinates(server), child)
            q.release("waiting")
            self.assertEqual(waiting.wait(timeout=3), 0)
            eventually(lambda: not alive(child[0]))
            self.assertEqual(caffeinates(server), [])
            self.assertTrue(any(line.startswith("done ") for line in self.listing(q, True)))

    @unittest.skipUnless(sys.platform == "darwin", "macOS caffeinate only")
    def test_sleep_prevention_server_exit(self):
        for action in ("shutdown", signal.SIGTERM, signal.SIGKILL):
            with self.subTest(action=action), Queue(1) as q:
                p = q.start("running")
                pids = q.pids("running")
                child = eventually(lambda: caffeinates(q.server()))[0]
                if action == "shutdown":
                    self.assertEqual(q.call("shutdown"), (0, b"", b""))
                else:
                    os.kill(q.server(), action)
                eventually(lambda: not alive(child))
                self.assertIsNone(p.poll())
                os.killpg(pids[0], signal.SIGTERM)
                self.assertEqual(p.wait(timeout=3), 143)


class ReviewFixTests(unittest.TestCase):
    def group_job(self, q, kind, delay=2):
        p = q.popen("run", PY, WORKER, "group", kind, str(delay))
        pids = q.pids("old")
        return p, pids

    def next_probe(self, q, group):
        return q.popen("run", PY, WORKER, "probe-group", str(group))

    def assert_probe_after_empty(self, q, probe):
        self.assertEqual(probe.wait(timeout=8), 0)
        record = json.loads((q.path / "probe").read_text())
        self.assertFalse(record["group_alive"], record)
        return record

    def test_slots_held_during_slow_termination(self):
        for action, kind in (("client-kill", "slow"), ("junban-kill", "child-slow")):
            with self.subTest(action=action), Queue(1) as q:
                p, pids = self.group_job(q, kind)
                probe = self.next_probe(q, pids[0])
                q.wait_rows(2)
                if action == "client-kill":
                    p.kill()
                else:
                    self.assertEqual(q.call("kill", "0"), (0, b"", b""))
                eventually(lambda: (q.path / "term").exists())
                self.assertIsNone(probe.poll())
                self.assertEqual(q.rows(), [(0, "running"), (1, "queued")])
                self.assertFalse((q.path / "done").exists())
                start = time.monotonic()
                self.assertEqual(q.call("ls")[0], 0)
                self.assertLess(time.monotonic() - start, 1)
                record = self.assert_probe_after_empty(q, probe)
                self.assertGreaterEqual(record["time"], float((q.path / "done").read_text()))
                self.assertEqual(p.wait(timeout=3), -9 if action == "client-kill" else 143)
                for pid in pids:
                    eventually(lambda pid=pid: not alive(pid))

    def test_sigterm_ignored_escalates_after_five_seconds(self):
        for action in ("client-kill", "junban-kill"):
            with self.subTest(action=action), Queue(1) as q:
                p, pids = self.group_job(q, "ignore")
                probe = self.next_probe(q, pids[0])
                q.wait_rows(2)
                started = time.monotonic()
                if action == "client-kill":
                    p.kill()
                else:
                    self.assertEqual(q.call("kill", "0"), (0, b"", b""))
                time.sleep(0.2)
                self.assertTrue(alive(pids[0]))
                self.assertIsNone(probe.poll())
                self.assertEqual(q.rows(), [(0, "running"), (1, "queued")])
                record = self.assert_probe_after_empty(q, probe)
                elapsed = record["time"] - started
                self.assertGreaterEqual(elapsed, 4.8)
                self.assertLess(elapsed, 7.5)
                self.assertEqual(p.wait(timeout=3), -9 if action == "client-kill" else 137)
                self.assertFalse(alive(pids[0]))

    def test_background_group_holds_slots_and_child_status(self):
        with Queue(1) as q:
            p, pids = self.group_job(q, "background")
            probe = self.next_probe(q, pids[0])
            q.wait_rows(2)
            eventually(lambda: not alive(pids[0]))
            self.assertTrue(alive(pids[1]))
            self.assertIsNone(p.poll())
            self.assertIsNone(probe.poll())
            self.assertEqual(q.rows(), [(0, "running"), (1, "queued")])
            record = self.assert_probe_after_empty(q, probe)
            self.assertGreaterEqual(record["time"], float((q.path / "done").read_text()))
            self.assertEqual(p.wait(timeout=3), 7)

    def test_forwarded_signal_does_not_escalate(self):
        with Queue(1) as q:
            p, pids = self.group_job(q, "child-ignore")
            probe = self.next_probe(q, pids[0])
            q.wait_rows(2)
            p.terminate()
            eventually(lambda: not alive(pids[0]))
            time.sleep(5.2)
            self.assertTrue(alive(pids[1]))
            self.assertIsNone(p.poll())
            self.assertIsNone(probe.poll())
            self.assertEqual(q.rows(), [(0, "running"), (1, "queued")])
            os.killpg(pids[0], signal.SIGKILL)
            self.assertEqual(p.wait(timeout=3), 143)
            self.assert_probe_after_empty(q, probe)

    def test_stalled_message_times_out_without_disconnect(self):
        for partial in ("header", "body"):
            with self.subTest(partial=partial), Queue(1) as q:
                running = q.start("running")
                q.pids("running")
                following = q.popen("run", "true")
                q.wait_rows(2)
                with connect_raw(q) as bad:
                    if partial == "header":
                        bad.sendall(MESSAGE.pack(TYPES["GET_VERSION"], *([0] * 9))[:4])
                    else:
                        send_message(bad, "NEWJOB", 5, 0, 1, 1, 0, 0, -1, 0, 0)
                    time.sleep(0.1)
                    q.release("running")
                    self.assertEqual(running.wait(timeout=3), 0)
                    listing = q.popen("ls")
                    # Leave the stalled connection open throughout recovery.
                    self.assertEqual(following.wait(timeout=6), 0)
                    out, err = listing.communicate(timeout=3)
                    self.assertEqual((listing.returncode, err), (0 if out else 1, b""))
                    self.assertRegex(out, rb"^(?:(?:running|queued) id=\d+ .*\n)*$")
                    self.assertEqual(bad.recv(1), b"")

    def test_stalled_snapshot_reader_times_out_without_disconnect(self):
        with Queue(1) as q:
            running = q.start("running")
            q.pids("running")
            waiting = [q.popen("run", "true") for _ in range(80)]
            q.wait_rows(81)
            with connect_raw(q) as bad:
                bad.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
                send_message(bad, "SNAPSHOT", 1)
                time.sleep(0.1)
                q.release("running")
                self.assertEqual(running.wait(timeout=3), 0)
                listing = q.popen("ls")
                out, err = listing.communicate(timeout=7)
                self.assertEqual((listing.returncode, err), (0 if out else 1, b""))
                self.assertRegex(out, rb"^(?:(?:running|queued) id=\d+ .*\n)*$")
                eventually(lambda: all(p.poll() == 0 for p in waiting), 8)

    def test_blocked_ls_stdout_does_not_hold_server(self):
        with Queue(1) as q:
            running = q.start("running")
            q.pids("running")
            waiting = [q.popen("run", "true") for _ in range(80)]
            q.wait_rows(81)
            reader, writer = os.pipe()
            try:
                os.set_blocking(writer, False)
                with contextlib.suppress(BlockingIOError):
                    while True:
                        os.write(writer, b"x" * 65536)
                os.set_blocking(writer, True)
                listing = q.popen("ls", stdout=writer)
                os.close(writer)
                writer = -1
                time.sleep(0.2)
                self.assertIsNone(listing.poll())
                q.release("running")
                self.assertEqual(running.wait(timeout=3), 0)
                eventually(lambda: all(p.poll() == 0 for p in waiting), 8)
                self.assertEqual(q.call("ls")[0], 1)
                self.assertIsNone(listing.poll(), "stdout must still be blocked")
                listing.kill()
                listing.wait(timeout=3)
            finally:
                os.close(reader)
                if writer >= 0:
                    os.close(writer)

    def test_blocked_watch_stdout_does_not_hold_server(self):
        with Queue(1) as q:
            running = q.start("running")
            q.pids("running")
            waiting = [q.popen("run", "true") for _ in range(80)]
            q.wait_rows(81)
            master, slave = pty.openpty()
            filler = os.open(os.ttyname(slave), os.O_WRONLY | os.O_NONBLOCK)
            try:
                fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 500, 80, 0, 0))
                with contextlib.suppress(BlockingIOError):
                    while True:
                        os.write(filler, b"x" * 65536)
                os.close(filler)
                filler = -1
                monitor = q.popen("watch", stdin=slave, stdout=slave)
                time.sleep(0.2)
                self.assertIsNone(monitor.poll())
                q.release("running")
                self.assertEqual(running.wait(timeout=3), 0)
                eventually(lambda: all(p.poll() == 0 for p in waiting), 8)
                self.assertEqual(q.call("ls")[0], 1)
                self.assertIsNone(monitor.poll(), "terminal output must still be blocked")
                monitor.kill()
                monitor.wait(timeout=3)
            finally:
                if filler >= 0:
                    os.close(filler)
                os.close(master)
                os.close(slave)


if __name__ == "__main__":
    # Make relative test socket paths short even in very deep artifact roots.
    os.chdir(ROOT)
    unittest.main(verbosity=2)
