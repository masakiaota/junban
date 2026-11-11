/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <errno.h>

#include "main.h"

/* from signals.c */
extern volatile sig_atomic_t signals_child_pid;

int process_group_alive(int pgid)
{
    if (pgid <= 0) return 0;
    if (kill(-pgid, 0) == 0) return 1;
    /* Fail closed on errors other than an absent group. */
    return errno != ESRCH;
}

/* A foreground command gets its own group while retaining the controlling
 * terminal. Ignore SIGTTOU only during terminal ownership transfers. */
static void terminal_group(pid_t group)
{
    struct sigaction ignore = {0}, old;
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    sigaction(SIGTTOU, &ignore, &old);
    tcsetpgrp(STDIN_FILENO, group);
    sigaction(SIGTTOU, &old, NULL);
}

/* Returns errorlevel */
static void run_parent(int ready_fd, int gate, int pid, struct Result *result)
{
    int status;
    int res;
    struct timeval starttv;
    struct msg ack;
    pid_t foreground = tcgetpgrp(STDIN_FILENO);

    res = read(ready_fd, &starttv, sizeof(starttv));
    if (res != sizeof(starttv))
        error("cannot read command readiness");
    close(ready_fd);

    /* Register the group before allowing exec. EOF on the gate prevents
     * unregistered work if the client is killed at any point before this. */
    signals_child_pid = pid;
    c_send_runjob_ok(pid);
    if (recv_msg(server_socket, &ack) != sizeof(ack) || ack.type != RUNJOB_ACK) {
        close(gate);
        kill(-pid, SIGTERM);
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
        error("queue disconnected before command start");
    }
    if (foreground == getpgrp()) terminal_group(pid);
    if (write(gate, "x", 1) != 1) {
        close(gate);
        kill(-pid, SIGTERM);
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
        if (foreground == getpgrp()) terminal_group(foreground);
        error("command startup failed");
    }
    close(gate);
    unblock_sigint_and_install_handler();
    do { res = waitpid(pid, &status, 0); } while (res < 0 && errno == EINTR);
    if (res < 0) error("cannot wait for command");
    /* The direct child's status is final, but its background descendants
     * still own the job's slots. Keep forwarding signals while waiting. */
    while (process_group_alive(pid)) {
        struct timespec interval = {0, 100000000};
        nanosleep(&interval, NULL);
    }
    if (foreground == getpgrp()) terminal_group(foreground);
    signals_child_pid = 0;

    /* Set the errorlevel */
    if (WIFEXITED(status))
    {
        result->errorlevel = WEXITSTATUS(status);
        result->died_by_signal = 0;
    }
    else if (WIFSIGNALED(status))
    {
        result->signal = WTERMSIG(status);
        result->errorlevel = 128 + result->signal;
        result->died_by_signal = 1;
    }
    else
    {
        result->died_by_signal = 0;
        result->errorlevel = -1;
    }


}

/* Signal child readiness only after its process group exists. The timestamp
 * payload is retained to avoid changing the startup synchronization. */
static void run_child(int ready_fd, int gate)
{
    struct timeval starttv;
    char token;
    ssize_t n;

    if (setpgid(0, 0) < 0) _exit(125);

    /* Readiness payload. */
    gettimeofday(&starttv, NULL);
    write(ready_fd, &starttv, sizeof(starttv));
    close(ready_fd);
    do { n = read(gate, &token, 1); } while (n < 0 && errno == EINTR);
    close(gate);
    if (n != 1) _exit(125);
    restore_sigmask();

    /* The group was created before the parent registered it with the server. */
    execvp(command_line.command.array[0], command_line.command.array);
}

int run_job(struct Result *res)
{
    int pid;
    int p[2];
    int gate[2];


    block_sigint();

    /* Child readiness pipe and server-registration gate. */
    if (pipe(p) < 0 || pipe(gate) < 0)
        error("cannot create command pipe: %s", strerror(errno));

    pid = fork();

    switch(pid)
    {
        case 0:
            close(server_socket);
            close(p[0]);
            close(gate[1]);
            run_child(p[1], gate[0]);
            /* Not reachable, if the 'exec' of the command
             * works. Thus, command exists, etc. */
            {
                int saved = errno;
                fprintf(stderr, "junban: cannot execute command: %s\n", strerror(saved));
                _exit(saved == ENOENT ? 127 : 126);
            }
        case -1:
            error("cannot fork command: %s", strerror(errno));
        default:
            close(p[1]);
            close(gate[0]);
            run_parent(p[0], gate[1], pid, res);
            break;
    }

    return res->errorlevel;
}

