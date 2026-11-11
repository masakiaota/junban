# junban

English | [日本語](README.ja.md)

junban is a tool for managing compute jobs on a single machine. It is handy when you have several AI agents working on Kaggle or competitive programming at the same time.

- Jobs wait in a queue according to how many CPUs they use. When CPUs are free, several jobs run at once, so the machine is not left idle.
- A job with no CPU count never runs alongside another job. Benchmarks that measure run time are not disturbed by other work.
- You only put `junban run` in front of a command. Input, output, environment variables and the exit status are the same as when you run the command directly.

It runs on macOS and Ubuntu, and is based on [task-spooler](https://viric.name/soft/ts/).

## Install

You need a C compiler and make.

```console
$ make
$ make install PREFIX=$HOME/.local
```

This installs `$HOME/.local/bin/junban`. Without `PREFIX`, it installs under `/usr/local`.

## Usage

### Run a job

Put `junban run` in front of the command. Give the number of CPUs it uses with `-c`.

```console
$ junban run -c 4 python train.py
```

Without `-c`, the job is treated as using every CPU on the machine. No other job runs while it is running. Run benchmarks that measure run time without `-c`.

```console
$ junban run python eval.py
```

If enough CPUs are free, the job starts right away. Otherwise it waits until they are. While it waits, junban shows what is going on:

```console
$ junban run python eval.py
junban: id=1 queued, cpus=all(4), 1 ahead
junban:   running id=0 dir=demo cpus=1 elapsed=0s est=30m
junban: id=1 started after 1s
```

These messages go to standard error. Standard output carries only the command's own output.

### Say how long it will take

You can tell junban how long you expect a job to take with `--estimate`. Write the time with a unit: `90s`, `30m`, `2h`, `1d`.

```console
$ junban run -c 8 --estimate 2h python train.py
```

The estimate is shown to jobs that are waiting. A job is never stopped for running past its estimate.

### Check the queue

`junban ls` lists the running and waiting jobs.

```console
$ junban ls
cpus 1/4 running 1 queued 1
running id=0 dir=demo cpus=1 elapsed=1s est=30m cmd=sleep 30
queued id=1 dir=demo cpus=all(4) waited=1s est=5m cmd=sh -c echo evaluated
```

`dir` is the name of the directory the job was started from. `cpus=all(4)` means the job was started without `-c`. A job that has run past its estimate is marked `over`. To see finished jobs as well, use `junban ls -a`.

For people, `junban watch` is easier to read. The screen refreshes every two seconds. Press `q` to quit.

```text
junban  cpus 3/4 [#########################---------] running 1 queued 1 q:quit
RUNNING
 ID DIR              CPUS ELAPSED EST      COMMAND
  1 AHC075-b            3 1s      1s OVER  sleep 30
QUEUED
 ID DIR              CPUS WAITED  EST      COMMAND
  2 AHC075-a          all 1s      5m       true
DONE
 ID DIR              CPUS ELAPSED RESULT      COMMAND
  0 AHC075-a          all 0s      exit=0      true
```

### Stop a job

Pass the job's ID to `junban kill`. A running job is stopped; a waiting job is removed from the queue.

```console
$ junban kill 0
```

If you interrupt `junban run` itself, with Ctrl-C for example, the command it is running stops too.

## Good to know

- Jobs start in the order they were submitted. Until the CPUs needed by the first job in the queue are free, the jobs behind it do not start. This keeps a large job from waiting forever while small jobs keep going ahead of it.
- `junban run a && b` puts only `a` in the queue. To queue both, write `junban run sh -c 'a && b'`.
- If a command exits and leaves child processes running in the background, `junban run` does not return until they finish.
- The exit status of `junban run` is the exit status of the command. For junban's own errors, such as not being able to reach the queue or a bad argument, it returns 125.
- macOS does not go to sleep while there are jobs in the queue.

## Instructions for AI agents

Put something like this in `AGENTS.md` or `CLAUDE.md`:

```text
- Always run long computations through junban run.
- Give the number of CPUs with -c, counting threads and child processes.
- Run benchmarks that measure run time without -c.
- When you can guess how long a job will take, say so with --estimate.
- When you have to wait, check junban ls. Do not stop other agents' jobs.
```

## Limitations

- junban is meant for one person using one machine. It does not coordinate with other users' jobs.
- It cannot manage work that is not started through junban. Such work can still run at the same time as a benchmark.
- The CPU count given with `-c` is taken on trust. A job that uses more is not restricted. Memory and GPUs are not managed.
- If the junban server stops, waiting jobs end with an error (exit status 125). Commands that are already running keep running.
- It handles up to 995 jobs at a time, running and waiting combined.
- Some sandbox settings prevent junban from reaching its server. In that case it reports an error and does not run the command.

## Tested on

Run the tests with `make test` (needs Python 3). junban has been tested on:

- macOS (Apple Silicon)
- Ubuntu 24.04 (arm64, in Docker)

## License

junban is a modified version of [task-spooler](https://viric.name/soft/ts/) 1.0.4 by Lluís Batlle i Rossell. It is licensed under the GNU General Public License version 2. See [COPYING](COPYING) for the full text.
