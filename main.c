/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009 Lluís Batlle i Rossell
    Please find the license in the provided COPYING file.
*/
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <errno.h>
#include <sys/time.h>
#include <fcntl.h>
#include "main.h"

struct Command_line command_line;
int server_socket = -1;

static void usage(void)
{
    error("usage: run [-c N | --cpus N] [-g N | --gpus N] [--estimate DUR] [--tag TAG] [--detach] [--] command [args...]; ls [-a] [--id N]... [--dir NAME]... [--tag TAG]... | ls --summary; wait [-v] [--timeout DUR] [--tag TAG]... [ID...]; watch; kill ID | kill --tag TAG; shutdown; help; version");
}

static unsigned long long duration(const char *s, const char *what)
{
    unsigned long long value = 0, factor;
    if (*s < '0' || *s > '9') error("%s must be a positive integer with unit s, m, h or d", what);
    while (*s >= '0' && *s <= '9') {
        if (value > (ULLONG_MAX - (unsigned)(*s - '0')) / 10)
            error("%s is too large", what);
        value = value * 10 + (unsigned)(*s++ - '0');
    }
    switch (*s) {
    case 's': factor = 1; break;
    case 'm': factor = 60; break;
    case 'h': factor = 3600; break;
    case 'd': factor = 86400; break;
    default: error("%s requires unit s, m, h or d", what); return 0;
    }
    if (s[1] || !value) error("%s must be a positive integer with unit s, m, h or d", what);
    if (value > ULLONG_MAX / factor) error("%s is too large", what);
    return value * factor;
}

static char *caller_label(void)
{
    size_t size = 256;
    char *cwd, *label, *slash;
    for (;;) {
        cwd = malloc(size);
        if (!cwd) error("out of memory");
        if (getcwd(cwd, size)) break;
        free(cwd);
        if (errno != ERANGE) error("cannot determine caller cwd");
        size *= 2;
    }
    slash = strrchr(cwd, '/');
    label = strdup(slash && slash[1] ? slash + 1 : cwd);
    free(cwd);
    if (!label) error("out of memory");
    return label;
}

/* Tags appear verbatim in ls and are matched exactly, so keep them to one
 * shell-safe word. */
static char *checked_tag(char *tag, const char *source)
{
    size_t n = strlen(tag);
    if (!n || n > 63 || strspn(tag, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                                   "0123456789._:@+-") != n)
        error("%s must be 1-63 characters of A-Z a-z 0-9 . _ : @ + -", source);
    return tag;
}

static int number(const char *s, int minimum)
{
    unsigned long n = 0;
    if (!*s) usage();
    for (; *s; ++s) {
        if (*s < '0' || *s > '9') usage();
        n = n * 10 + (*s - '0');
        if (n > INT_MAX) usage();
    }
    if (n < (unsigned)minimum) usage();
    return (int)n;
}

int main(int argc, char **argv)
{
    int i = 2, status = 0, fd = -1;
    process_type = CLIENT;
    if (argc < 2) usage();
    if (!strcmp(argv[1], "help") || !strcmp(argv[1], "--help")) {
        if (argc != 2) usage();
        puts("Usage:");
        puts("  junban run [-c N] [-g N] [--estimate DUR] [--tag TAG] [--detach] [--] COMMAND [ARGS...]");
        puts("      Wait for a turn, then run COMMAND as if it were run directly. Returns when it ends.");
        puts("      -c N, --cpus N   CPUs the job uses. Without it, the job takes all CPUs and runs alone.");
        puts("      -g N, --gpus N   GPUs the job uses: 0 or 1. Without it, 0. Neither option changes");
        puts("                       the other: a GPU job that needs little CPU gives both, as -c 1 -g 1.");
        puts("      --estimate DUR   Expected duration: 90s, 30m, 2h, 1d. Shown to others. Never a timeout.");
        puts("      --tag TAG        Label the job as yours. Default: $JUNBAN_TAG.");
        puts("                       1-63 characters of A-Z a-z 0-9 . _ : @ + -");
        puts("      --detach         Queue the job, print only its ID on stdout, and return at once.");
        puts("                       Its stdout and stderr go to <queue socket>.logs/ID.log, shown as log= in ls.");
        puts("  junban wait [-v] [--timeout DUR] [--tag TAG]... [ID...]");
        puts("      Block until the jobs finish. Already finished jobs are judged from kept results.");
        puts("      --tag TAG        Also wait for the tagged jobs running or queued when the wait");
        puts("                       starts or queued while it waits.");
        puts("      --timeout DUR    Give up after DUR (90s, 30m, 2h, 1d) with exit status 124.");
        puts("      -v               Print one line per finished job: finished id=N exit=N elapsed=DUR.");
        puts("  junban ls [-a] [--id N]... [--dir NAME]... [--tag TAG]...");
        puts("      List running and queued jobs. -a adds the last 20 finished jobs.");
        puts("      --id N, --dir NAME, --tag TAG   Only matching jobs. Each option is repeatable;");
        puts("                       a job must match one value of every option given.");
        puts("  junban ls --summary  Print only the queue totals: cpus BUSY/TOTAL gpus BUSY/TOTAL running N queued N");
        puts("  junban watch         Live view for a terminal. q quits.");
        puts("  junban kill ID       Stop a running job, or remove a queued one.");
        puts("  junban kill --tag TAG   Do that to every running and queued job tagged TAG.");
        puts("  junban shutdown      Stop the queue server.");
        puts("");
        puts("Waiting for jobs:");
        puts("  Run one job in the foreground: junban run returns when it ends.");
        puts("  For parallel jobs: id=$(junban run --detach -c N CMD) for each, then junban wait ID...");
        puts("  Do not loop over the output of junban ls; it lists other agents' jobs too.");
        puts("");
        puts("Output of ls, meant for scripts:");
        puts("  stdout has one line per job and nothing else; an empty queue prints nothing.");
        puts("  STATE id=N dir=DIR [tag=TAG] cpus=C gpus=G TIME [est=DUR [over]] [RESULT] [log=PATH] cmd=COMMAND");
        puts("  STATE                  running, queued, or done (finished; with -a).");
        puts("  id, dir, cmd           Job ID, directory it was started from, command (always last).");
        puts("  tag, log               The job's tag; output file of a --detach job. Shown when set.");
        puts("  cpus=N, cpus=all(N)    all(N): -c was not given, so the job takes all N CPUs.");
        puts("  gpus=N                 GPUs the job uses (-g); 0 when not given.");
        puts("  elapsed=, waited=, est=   Time running (waited= while queued), declared estimate.");
        puts("  over                   The job has run longer than its estimate.");
        puts("  exit=N, signal=N, killed, client-lost   RESULT: how a finished job ended.");
        puts("");
        puts("Exit status:");
        puts("  run    The command's own, or 128+N if signal N killed it. 125: junban could not");
        puts("         queue or run it. 126: not executable. 127: not found.");
        puts("  wait   The highest exit status of the jobs (0 if all succeeded), 124 on timeout.");
        puts("  ls     0 if it listed a job, 1 if it listed none.");
        puts("  kill --tag   1 if no job had the tag.");
        puts("  All commands exit with 125 on junban errors, such as a bad argument or unknown ID.");
        return 0;
    }
    if (!strcmp(argv[1], "version") || !strcmp(argv[1], "--version")) {
        if (argc != 2) usage();
        puts("junban 0.1.1 (based on task-spooler 1.0.4)");
        return 0;
    }
    if (!strcmp(argv[1], "run")) {
        const char *jobid = getenv("JUNBAN_JOB_ID");
        if (jobid)
            error("'junban run' cannot be called inside a junban job (job %s)", jobid);
        char *tag = NULL;
        command_line.request = c_QUEUE;
        /* Parse only the prefix; never permute the command's arguments. */
        while (i < argc && argv[i][0] == '-') {
            if (!strcmp(argv[i], "--")) { ++i; break; }
            if (!strcmp(argv[i], "--detach")) {
                if (command_line.detach) usage();
                command_line.detach = 1;
                ++i;
                continue;
            }
            if (!strcmp(argv[i], "--tag")) {
                if (tag || ++i == argc) usage();
                command_line.tag = tag = checked_tag(argv[i++], "tag");
                continue;
            }
            if (!strcmp(argv[i], "--estimate")) {
                if (command_line.estimate || ++i == argc) usage();
                command_line.estimate = duration(argv[i++], "estimate");
                continue;
            }
            if (!strcmp(argv[i], "-g") || !strcmp(argv[i], "--gpus")) {
                if (command_line.gpus_given || ++i == argc) usage();
                command_line.gpus_given = 1;
                command_line.num_gpus = number(argv[i++], 0);
                continue;
            }
            if (strcmp(argv[i], "-c") && strcmp(argv[i], "--cpus")) usage();
            if (command_line.num_slots || ++i == argc) usage();
            command_line.num_slots = number(argv[i++], 1);
        }
        if (i == argc) usage();
        /* JUNBAN_TAG is only a default for an omitted --tag. */
        if (!tag && (tag = getenv("JUNBAN_TAG")) && *tag)
            command_line.tag = checked_tag(tag, "JUNBAN_TAG");
        command_line.command.array = argv + i;
        command_line.command.num = argc - i;
        command_line.label = caller_label();
    } else if (!strcmp(argv[1], "ls")) {
        command_line.request = c_LIST;
        command_line.ids = malloc(sizeof(int) * (size_t)argc);
        command_line.dirs = malloc(sizeof(char *) * (size_t)argc);
        command_line.tags = malloc(sizeof(char *) * (size_t)argc);
        if (!command_line.ids || !command_line.dirs || !command_line.tags)
            error("out of memory");
        for (; i < argc; ++i) {
            if (!strcmp(argv[i], "-a") && !command_line.list_all)
                command_line.list_all = 1;
            else if (!strcmp(argv[i], "--summary") && !command_line.list_summary)
                command_line.list_summary = 1;
            else if (!strcmp(argv[i], "--id") && i + 1 < argc)
                command_line.ids[command_line.num_ids++] = number(argv[++i], 0);
            else if (!strcmp(argv[i], "--dir") && i + 1 < argc && argv[i + 1][0])
                command_line.dirs[command_line.num_dirs++] = argv[++i];
            else if (!strcmp(argv[i], "--tag") && i + 1 < argc)
                command_line.tags[command_line.num_tags++] = checked_tag(argv[++i], "tag");
            else usage();
        }
        /* The summary describes the whole queue; it takes no job selection. */
        if (command_line.list_summary &&
            (command_line.list_all || command_line.num_ids || command_line.num_dirs ||
             command_line.num_tags))
            usage();
    } else if (!strcmp(argv[1], "wait")) {
        command_line.request = c_WAIT;
        command_line.ids = malloc(sizeof(int) * (size_t)argc);
        command_line.tags = malloc(sizeof(char *) * (size_t)argc);
        if (!command_line.ids || !command_line.tags) error("out of memory");
        for (; i < argc; ++i) {
            if ((!strcmp(argv[i], "-v") || !strcmp(argv[i], "--verbose")) && !command_line.verbose)
                command_line.verbose = 1;
            else if (!strcmp(argv[i], "--timeout") && i + 1 < argc && !command_line.timeout)
                command_line.timeout = duration(argv[++i], "timeout");
            else if (!strcmp(argv[i], "--tag") && i + 1 < argc)
                command_line.tags[command_line.num_tags++] = checked_tag(argv[++i], "tag");
            else
                command_line.ids[command_line.num_ids++] = number(argv[i], 0);
        }
        if (!command_line.num_ids && !command_line.num_tags) usage();
    } else if (!strcmp(argv[1], "watch") && argc == 2) {
        if (!isatty(STDOUT_FILENO)) error("watch requires a terminal; use junban ls");
        command_line.request = c_WATCH;
    } else if (!strcmp(argv[1], "shutdown") && argc == 2)
        command_line.request = c_KILL_SERVER;
    else if (!strcmp(argv[1], "kill") && argc == 4 && !strcmp(argv[2], "--tag")) {
        command_line.request = c_KILL_TAG;
        command_line.tags = argv + 3;
        command_line.num_tags = 1;
        checked_tag(argv[3], "tag");
    } else if (!strcmp(argv[1], "kill") && argc == 3) {
        command_line.request = c_KILL_JOB;
        command_line.jobid = number(argv[2], 0);
    } else usage();

    ignore_sigpipe();
    unblock_sigint_and_install_handler();
    /* A detached job replaces 0-2, so nothing junban opens may land there
     * when the caller closed them. Reopened read-only, a closed stdout
     * still fails to take the job ID. */
    if (command_line.detach)
        while ((fd = open("/dev/null", O_RDONLY)) >= 0 && fd <= STDERR_FILENO) {}
    if (fd > STDERR_FILENO) close(fd);
    ensure_server_up();
    /* KILL_SERVER keeps its message number, so shutdown also stops a server
     * of another protocol version, as needed when upgrading junban. */
    if (command_line.request == c_WAIT) c_wait_prepare();
    if (command_line.request != c_KILL_SERVER) c_check_version();
    switch (command_line.request) {
    case c_QUEUE:
        c_get_max_slots();
        if (command_line.num_slots > command_line.max_slots)
            error("requested CPUs exceed queue total (%d)", command_line.max_slots);
        if (command_line.num_gpus > command_line.max_gpus)
            error("requested GPUs exceed queue total (%d)", command_line.max_gpus);
        if (command_line.detach) command_line.log_prefix = c_log_prefix();
        c_new_job();
        command_line.jobid = c_wait_newjob_ok();
        if (command_line.detach) c_detach();
        status = c_wait_server_commands();
        break;
    case c_LIST: {
        struct Snapshot snapshot = {0};
        if (!fetch_snapshot(&snapshot)) error("queue disconnected while listing");
        /* Like grep: 0 when a job matched, 1 when none did. */
        if (command_line.list_summary) print_summary(&snapshot);
        else status = print_snapshot(&snapshot) ? 0 : 1;
        free(snapshot.jobs);
        break;
    }
    case c_WATCH: c_watch(); break;
    case c_KILL_SERVER: c_shutdown_server(); break;
    case c_KILL_JOB: c_kill_job(); break;
    case c_WAIT: status = c_wait(); break;
    case c_KILL_TAG:
        /* Like pkill: 1 when no job carried the tag. */
        if (!c_kill_tag()) {
            fprintf(stderr, "junban: no running or queued job with tag %s\n", command_line.tags[0]);
            status = 1;
        }
        break;
    default: usage();
    }
    close(server_socket);
    free(command_line.label);
    return status;
}
