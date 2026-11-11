# Modified for junban in 2026 by junban contributors.
"""Small real commands used by test_queue.py (standard library only)."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import termios
import time

mode = sys.argv[1]
if mode == "watch-host":
    # Retain the controlling-terminal session after watch exits. macOS
    # revokes the slave when its session leader exits, preventing inspection.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    before = termios.tcgetattr(0)
    child = subprocess.Popen([sys.argv[2], "watch"])
    def stop_host(signum, _frame):
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=3)
        sys.exit(128 + signum)
    signal.signal(signal.SIGTERM, stop_host)
    code = child.wait()
    after = termios.tcgetattr(0)
    # PENDIN is kernel input-reprocessing state, set when returning from
    # noncanonical input on macOS. Compare all actual configuration bits.
    before[3] &= ~getattr(termios, "PENDIN", 0)
    after[3] &= ~getattr(termios, "PENDIN", 0)
    restored = after == before
    print("WATCH-EXIT:%d MODES:%s" % (code, "restored" if restored else "changed"), flush=True)
    time.sleep(120)
elif mode == "group":
    kind, delay = sys.argv[2], float(sys.argv[3])
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    leader = os.getpid()
    def finish_group(_signum, _frame):
        Path("term").write_text(str(time.monotonic()))
        time.sleep(delay)
        Path("done").write_text(str(time.monotonic()))
        os._exit(0)
    if kind == "slow":
        signal.signal(signal.SIGTERM, finish_group)
        Path("old.pids").write_text(json.dumps([leader]))
    elif kind == "ignore":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        Path("old.pids").write_text(json.dumps([leader]))
    else:
        child = os.fork()
        if child == 0:
            if kind == "child-slow":
                signal.signal(signal.SIGTERM, finish_group)
            elif kind == "child-ignore":
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            Path("old.pids").write_text(json.dumps([leader, os.getpid()]))
            if kind == "background":
                time.sleep(delay)
                Path("done").write_text(str(time.monotonic()))
                os._exit(0)
        else:
            while not Path("old.pids").exists():
                time.sleep(0.005)
            if kind == "background":
                os._exit(7)
    while True:
        time.sleep(0.1)
elif mode == "probe-group":
    try:
        os.killpg(int(sys.argv[2]), 0)
        group_alive = True
    except ProcessLookupError:
        group_alive = False
    Path("probe").write_text(json.dumps({"group_alive": group_alive,
                                       "time": time.monotonic()}))
elif mode == "transparent":
    print(json.dumps({"cwd": os.getcwd(), "mark": os.getenv("MARK"),
                      "only": os.getenv("CALLER_ONLY"),
                      "server_only": os.getenv("SERVER_ONLY"),
                      "posix": os.getenv("POSIXLY_CORRECT"),
                      "stdin": sys.stdin.read()}), flush=True)
    os.write(2, b"separate stderr\n")
elif mode == "stream":
    os.write(1, b"O1\n")
    os.write(2, b"E1\n")
    time.sleep(0.5)
    os.write(1, b"O2\n")
    os.write(2, b"E2\n")
    time.sleep(0.5)
elif mode == "tty":
    print("TTY:" + ",".join(str(int(os.isatty(i))) for i in range(3)), flush=True)
    print("FG:" + str(int(os.tcgetpgrp(0) == os.getpgrp())), flush=True)
    print("READ:" + input(), flush=True)
elif mode in ("hold", "tree"):
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    child = subprocess.Popen(["sleep", "120"]) if mode == "tree" else None
    record = Path(sys.argv[2])
    record.write_text(json.dumps([os.getpid()] + ([child.pid] if child else [])))
    if mode == "tree":
        child.wait()
    else:
        while not Path(sys.argv[3]).exists():
            time.sleep(0.01)
elif mode == "event":
    Path(sys.argv[2]).write_text("started")
elif mode == "race":
    # Atomic exclusive creation detects overlap despite scheduler/timer noise.
    target = sys.argv[2]
    fd = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    time.sleep(0.03)
    os.unlink(target)
else:
    raise RuntimeError(mode)
