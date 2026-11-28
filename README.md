# junban

English | [日本語](README.ja.md)

junban is a tool for managing compute jobs on a single machine. It is handy when you have several AI agents working on Kaggle or competitive programming at the same time.

- Jobs wait in a queue according to how many CPUs and GPUs they use. When they are free, several jobs run at once, so the machine is not left idle.
- A job with no CPU count never runs alongside another job. Benchmarks that measure run time are not disturbed by other work.
- You only put `junban run` in front of a command. Input, output, environment variables and the exit status are the same as when you run the command directly.

It runs on macOS and Ubuntu, and is based on [task-spooler](https://viric.name/soft/ts/).

## Install

With Homebrew on macOS:

```console
$ brew install masakiaota/tap/junban
```

Or build from source (requires a C compiler and make):

```console
$ make
$ make install PREFIX=$HOME/.local
```

This installs `$HOME/.local/bin/junban`. Without `PREFIX`, it installs under `/usr/local`.

### Upgrade

Upgrade junban the way you installed it, with `brew upgrade junban` or `make install` again. The running server notices the new junban the next time any junban command connects to it, and switches to it in place. Running and queued jobs, finished results and job IDs are kept, and waiting commands stay connected.

If the new junban speaks a different protocol, as 0.6.0 does, the server keeps running the old one. Stop it with `junban shutdown` when no jobs are running or queued. A server older than 0.4.0 cannot switch either, so after upgrading from one, run `junban shutdown` once at such a time.

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

### Run a GPU job

Give the number of GPUs a job uses with `-g` (`--gpus`). junban treats the GPU as one device, so `-g` is 0 or 1. Without `-g`, the job uses no GPU. Only one job with `-g 1` runs at a time.

`-c` and `-g` do not change each other's default. `-g 1` alone still takes all CPUs, so the job runs alone. A GPU job that needs little CPU, such as training with MPS or MLX, should give both, so that CPU jobs can run beside it. It then also waits ahead of them (see [Good to know](#good-to-know)):

```console
$ junban run -c 1 -g 1 python train_mps.py
```

| Options | CPUs | GPUs |
|---|---|---|
| none | all | 0 |
| `-c 4` | 4 | 0 |
| `-c 1 -g 1` | 1 | 1 |
| `-g 1` | all | 1 |

If enough CPUs are free, the job starts right away. Otherwise it waits until they are. While it waits, junban shows what is going on, including whether it waits for CPUs, GPUs or both, and the latest time it starts, once junban can tell:

```console
$ junban run python eval.py
junban: id=1 queued, cpus=all(4), gpus=0, 1 ahead, waiting for cpus
junban:   running id=0 dir=demo cpus=1 gpus=0 elapsed=0s est=30m
junban: id=1 queued, to start by 14:50
junban: id=1 started after 1s
```

These messages go to standard error. Standard output carries only the command's own output.

### Run a job in the background

`junban run` returns when the job ends. To run several jobs in parallel, use `--detach`. It puts the job in the queue, prints only the job ID on standard output, and returns at once:

```console
$ id=$(junban run --detach -c 8 python train.py)
junban: id=63 log=/Users/me/Library/Caches/junban/queue.sock.logs/63.log
```

The job then waits for its turn and runs as usual. Its standard output and standard error go to the file `queue.sock.logs/ID.log` in the queue directory (`~/Library/Caches/junban` on macOS; `$XDG_RUNTIME_DIR/junban` or `~/.cache/junban` on Ubuntu). The path is also shown as `log=` in `junban ls`. Its standard input is `/dev/null`. Wait for it with `junban wait`, described next. Job IDs start again from 0 when the junban server restarts, and a new job then overwrites an old log with the same ID.

### Wait for jobs

`junban wait` blocks until the given jobs have finished. Its exit status is the highest exit status of those jobs, so it is 0 when all of them succeeded:

```console
$ a=$(junban run --detach -c 4 python train.py --fold 0)
$ b=$(junban run --detach -c 4 python train.py --fold 1)
$ junban wait "$a" "$b"
```

- Jobs that have already finished are judged at once from their results. junban keeps the results of the last 1000 finished jobs.
- `--timeout DUR` gives up after `DUR` (`90s`, `30m`, `2h`, `1d`) and exits with 124. The jobs keep running.
- `--tag TAG` waits for every job with that tag that is running or queued when the wait starts, or is queued while it waits. Jobs with the tag that had already finished are not included. To know each job's result, wait by ID.
- By default it prints nothing. With `-v`, it prints one line on standard output each time a job finishes: `finished id=63 exit=0 elapsed=4m12s`.
- It exits with 125 for junban's own errors, such as an unknown job ID or not being able to reach the queue.

Wait with `junban wait`, not with a loop around `junban ls`. A loop over the whole listing also waits for other agents' jobs.

### Say how long it will take

You can tell junban how long you expect a job to take with `--estimate`. Write the time with a unit: `90s`, `30m`, `2h`, `1d`.

```console
$ junban run -c 8 --estimate 2h python train.py
```

The estimate is shown to jobs that are waiting. A job is never stopped for running past its estimate.

An estimate lets junban use idle CPUs: a job with one can start ahead of waiting jobs, and can let others start ahead of it, within limits (see [Good to know](#good-to-know)). Do not give a shorter time than you expect: a job that runs past its estimate delays the jobs it went ahead of.

### Tag your jobs

`--tag` puts a tag on a job, so you can treat your jobs as a group. If the environment variable `JUNBAN_TAG` is set, jobs get that tag without `--tag`. Set it once per agent session and its jobs are grouped automatically. A tag is 1 to 63 characters from `A-Z a-z 0-9 . _ : @ + -`.

```console
$ export JUNBAN_TAG=agent-1
$ junban run -c 4 python train.py
$ junban ls --tag agent-1
running id=5 dir=demo tag=agent-1 cpus=4 gpus=0 elapsed=10s cmd=python train.py
$ junban kill --tag agent-1
```

`junban kill --tag TAG` stops every running and queued job with that tag. It exits with 1 when there is no such job.

### Check the queue

`junban ls` lists the running and waiting jobs, one job per line.

```console
$ junban ls
running id=0 dir=demo cpus=1 gpus=0 elapsed=1s est=30m cmd=sleep 30
queued id=1 dir=demo cpus=all(4) gpus=0 waited=1s est=5m start-by=14:50 cmd=sh -c echo evaluated
```

`dir` is the name of the directory the job was started from. `cpus=all(4)` means the job was started without `-c`. `gpus=` is the number given with `-g`, 0 without it. A job that has run past its estimate is marked `over`. `start-by=` is the latest a queued job starts as things stand: its planned start plus the slack it has left (see [Good to know](#good-to-know)), with the date before the time (`11-28T09:15`) unless it is today. It moves when a running job ends early or runs past its estimate, or when a GPU job lines up ahead, and it is missing while the job waits for a job without an estimate to end. To see finished jobs as well, use `junban ls -a`; their lines start with `done`.

Standard output of `junban ls` carries job lines only, and every line starts with the job's state: `running`, `queued` or `done`. When there are no jobs, it prints nothing. Like `grep`, `junban ls` exits with 0 when it listed at least one job, 1 when it listed none, and 125 for junban's own errors. To see how many CPUs and GPUs are in use, use `junban ls --summary`:

```console
$ junban ls --summary
cpus 1/4 gpus 0/1 running 1 queued 1
```

To list only some jobs, use `--id N` for a job ID, `--dir NAME` for the directory shown as `dir=`, and `--tag TAG` for a tag. Both can be repeated, and they can be combined with each other and with `-a`. The exit status then tells whether any of those jobs is listed:

```console
$ junban ls --id 63 --id 64
running id=63 dir=demo cpus=8 gpus=0 elapsed=5m cmd=python train.py
$ junban ls -a --dir demo
```

For people, `junban watch` is easier to read. The screen refreshes every two seconds. Press `q` to quit.

```text
junban                                                         14:32:05  q:quit
 CPU ████████████████████████████████████░░░░░░░░░░░░  3/4   running 2
 GPU ████████████████████████████████████████████████  1/1   queued  1

RUNNING 2
  1  python train.py --lr 0.1 --epochs 50
     AHC075-b/  12m / 30m ━━━━━━━──────────── 18m left  cpus 2 gpus 1  #sweep
  3  sleep 30
     AHC075-a/  45s / 30s ━━━━━━━━━━━━━━━━━━ OVER +15s  cpus 1

QUEUED 1
  4  python eval.py --ckpt best.pt
     AHC075-b/  waited 40s  est 10m  by 14:50           cpus 1 gpus 1  #sweep

DONE
✗ 2  pytest -x
     AHC075-a/  exit 1   42s  ended 14:25
✓ 0  true
     AHC075-a/  ok       0s  ended 14:20
```

Each job takes two rows: its ID and command, then its directory, timing, the CPUs and GPUs it holds, and its tag. The directory column widens to fit the names on screen, up to 32 columns. A running job with an estimate shows a progress bar and the time left, or `OVER` once past it; a queued one shows its estimate and the latest time it starts. A finished job shows `✓` or `✗`, its result, and the time it ended. Colour marks meaning only: CPUs are cyan, GPUs magenta, directories blue, and anything wrong red. `NO_COLOR=1` turns colour off, and without a UTF-8 locale the screen uses ASCII only, with `?` for each non-ASCII character in job text. On a narrow terminal the tag is dropped first, then the timing shrinks; each piece shows whole or not at all.

### Stop a job

Pass the job's ID to `junban kill`. A running job is stopped; a waiting job is removed from the queue.

```console
$ junban kill 0
```

If you interrupt `junban run` itself, with Ctrl-C for example, the command it is running stops too.

## Good to know

- Jobs start in the order of the line. A later job starts first only if that costs each waiting job no more than its slack.
  - Line: a job joins the end of the line. A job that uses the GPU instead lines up ahead of the waiting jobs that can run beside it, that is, whose CPUs and its own fit in the machine together. So the GPU is not left idle behind CPU jobs. They give up only the CPUs of one GPU job, since only one runs at a time. It stays behind earlier GPU jobs and behind a job that cannot run beside it, such as one started without `-c`. `junban ls` lists the waiting jobs in the order of the line.
  - Slack: a fifth of the time given with `--estimate`. A job started without `-c` (it takes all CPUs and is often a benchmark) and a job without an estimate have none: a job starts ahead of them only if junban's plan then starts them no later.
  - Cost: junban plans when each waiting job can start, from the estimates. What a job that starts ahead costs a waiting job is how much later that start becomes. Over all jobs that start ahead of it, this never adds up to more than its slack.
  - Without an estimate: junban counts only on what it was told, so such a job may run for good. It starts ahead of a waiting job only if all jobs without an estimate together still leave that job what it waits for. A job that waits only for the GPU is left no CPUs: the job that holds the GPU gives its CPUs back with it. A job that waits for one of them to end has no planned start yet, and jobs with an estimate may start ahead of it. A job started without `-c` is the exception: while it has no planned start, nothing starts ahead of it.
  - Overruns: a job past its estimate is expected to end a minute later, then 15, 30 and 60 minutes later, and so on. This moves the plan but uses up no one's slack.
  - Tags and directories never change the order.
- `junban run a && b` puts only `a` in the queue. To queue both, write `junban run sh -c 'a && b'`.
- You cannot call `junban run` from inside a job that junban is running. It fails with an error (exit status 125). This keeps a job from waiting for itself and blocking the queue.
- If a command exits and leaves child processes running in the background, `junban run` does not return until they finish.
- The exit status of `junban run` is the exit status of the command. For junban's own errors, such as not being able to reach the queue or a bad argument, it returns 125.
- macOS does not go to sleep while there are jobs in the queue.

## Instructions for AI agents

Put something like this in `AGENTS.md` or `CLAUDE.md`:

```text
- Always run long computations through junban run.
- Give the number of CPUs with -c, counting threads and child processes.
- Run benchmarks that measure run time without -c.
- Give -g 1 to jobs that use the GPU. Give -c as well, for example -c 1 -g 1.
- When you can guess how long a job will take, say so with --estimate. A job
  with an estimate can start early in idle CPUs. Do not give a shorter time
  than you expect.
- junban run in the foreground does not return until the job ends. To run jobs
  in parallel, start each with id=$(junban run --detach ...) and pass the IDs
  to junban wait. Its exit status is the highest exit status of the jobs.
- Never wait with a loop that greps the output of junban ls.
- Do not stop other agents' jobs.
```

If each agent session sets its own `JUNBAN_TAG` (for example `export JUNBAN_TAG=agent-1`), you can add a line such as `To stop all of your jobs, use junban kill --tag "$JUNBAN_TAG".`

## Limitations

- junban is meant for one person using one machine. It does not coordinate with other users' jobs.
- It cannot manage work that is not started through junban. Such work can still run at the same time as a benchmark.
- The CPU and GPU counts given with `-c` and `-g` are taken on trust. A job that uses more is not restricted. Memory is not managed.
- The GPU is managed as one device. junban does not detect GPUs or assign a GPU to a job, so on a machine with several GPUs it still runs one `-g 1` job at a time.
- If the junban server stops, waiting jobs end with an error (exit status 125). Commands that are already running keep running.
- It handles up to 995 jobs at a time, running and waiting combined.
- Some sandbox settings prevent junban from reaching its server. In that case it reports an error and does not run the command.

## Tested on

Run the tests with `make test` (needs Python 3). junban has been tested on:

- macOS (Apple Silicon)
- Ubuntu 24.04 (arm64, in Docker)

## License

junban is a modified version of [task-spooler](https://viric.name/soft/ts/) 1.0.4 by Lluís Batlle i Rossell. It is licensed under the GNU General Public License version 2. See [COPYING](COPYING) for the full text.
