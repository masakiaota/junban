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
#include "main.h"

struct Command_line command_line;
int server_socket = -1;

static void usage(void)
{
    error("usage: run [-c N | --cpus N] [--estimate DUR] [--] command [args...]; ls [-a]; watch; kill ID; shutdown; help; version");
}

static unsigned long long duration(const char *s)
{
    unsigned long long value = 0, factor;
    if (*s < '0' || *s > '9') error("estimate must be a positive integer with unit s, m, h or d");
    while (*s >= '0' && *s <= '9') {
        if (value > (ULLONG_MAX - (unsigned)(*s - '0')) / 10)
            error("estimate is too large");
        value = value * 10 + (unsigned)(*s++ - '0');
    }
    switch (*s) {
    case 's': factor = 1; break;
    case 'm': factor = 60; break;
    case 'h': factor = 3600; break;
    case 'd': factor = 86400; break;
    default: error("estimate requires unit s, m, h or d"); return 0;
    }
    if (s[1] || !value) error("estimate must be a positive integer with unit s, m, h or d");
    if (value > ULLONG_MAX / factor) error("estimate is too large");
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
    int i = 2, status = 0;
    process_type = CLIENT;
    if (argc < 2) usage();
    if (!strcmp(argv[1], "help") || !strcmp(argv[1], "--help")) {
        if (argc != 2) usage();
        puts("Usage:");
        puts("  junban run [-c N] [--estimate DUR] [--] COMMAND [ARGS...]");
        puts("      Wait for a turn, then run COMMAND as if it were run directly.");
        puts("      -c N, --cpus N   CPUs the job uses. Without it, the job takes all CPUs and runs alone.");
        puts("      --estimate DUR   Expected duration: 90s, 30m, 2h, 1d. Shown to others. Never a timeout.");
        puts("  junban ls [-a]     List running and queued jobs. -a adds the last 20 finished jobs.");
        puts("  junban watch       Live view for a terminal. q quits.");
        puts("  junban kill ID     Stop a running job, or remove a queued one.");
        puts("  junban shutdown    Stop the queue server.");
        puts("");
        puts("Fields in ls:");
        puts("  id, dir, cmd           Job ID, directory it was started from, command.");
        puts("  cpus=N, cpus=all(N)    all(N): -c was not given, so the job takes all N CPUs.");
        puts("  elapsed, waited, est   Time running, time in the queue, declared estimate.");
        puts("  over                   The job has run longer than its estimate.");
        puts("  exit=N, signal=N, killed, client-lost   How a finished job ended.");
        puts("");
        puts("Exit status of run: the command's own, or 128+N if signal N killed it.");
        puts("  125: junban could not queue or run it. 126: not executable. 127: not found.");
        return 0;
    }
    if (!strcmp(argv[1], "version") || !strcmp(argv[1], "--version")) {
        if (argc != 2) usage();
        puts("junban 0.1.0 (based on task-spooler 1.0.4)");
        return 0;
    }
    if (!strcmp(argv[1], "run")) {
        command_line.request = c_QUEUE;
        /* Parse only the prefix; never permute the command's arguments. */
        while (i < argc && argv[i][0] == '-') {
            if (!strcmp(argv[i], "--")) { ++i; break; }
            if (!strcmp(argv[i], "--estimate")) {
                if (command_line.estimate || ++i == argc) usage();
                command_line.estimate = duration(argv[i++]);
                continue;
            }
            if (strcmp(argv[i], "-c") && strcmp(argv[i], "--cpus")) usage();
            if (command_line.num_slots || ++i == argc) usage();
            command_line.num_slots = number(argv[i++], 1);
        }
        if (i == argc) usage();
        command_line.command.array = argv + i;
        command_line.command.num = argc - i;
        command_line.label = caller_label();
    } else if (!strcmp(argv[1], "ls") &&
               (argc == 2 || (argc == 3 && !strcmp(argv[2], "-a")))) {
        command_line.request = c_LIST;
        command_line.list_all = argc == 3;
    } else if (!strcmp(argv[1], "watch") && argc == 2) {
        if (!isatty(STDOUT_FILENO)) error("watch requires a terminal; use junban ls");
        command_line.request = c_WATCH;
    } else if (!strcmp(argv[1], "shutdown") && argc == 2)
        command_line.request = c_KILL_SERVER;
    else if (!strcmp(argv[1], "kill") && argc == 3) {
        command_line.request = c_KILL_JOB;
        command_line.jobid = number(argv[2], 0);
    } else usage();

    ignore_sigpipe();
    unblock_sigint_and_install_handler();
    ensure_server_up();
    c_check_version();
    switch (command_line.request) {
    case c_QUEUE:
        c_get_max_slots();
        if (command_line.num_slots > command_line.max_slots)
            error("requested CPUs exceed queue total (%d)", command_line.max_slots);
        c_new_job();
        command_line.jobid = c_wait_newjob_ok();
        status = c_wait_server_commands();
        break;
    case c_LIST: {
        struct Snapshot snapshot = {0};
        if (!fetch_snapshot(&snapshot)) error("queue disconnected while listing");
        print_snapshot(&snapshot);
        free(snapshot.jobs);
        break;
    }
    case c_WATCH: c_watch(); break;
    case c_KILL_SERVER: c_shutdown_server(); break;
    case c_KILL_JOB: c_kill_job(); break;
    default: usage();
    }
    close(server_socket);
    free(command_line.label);
    return status;
}
