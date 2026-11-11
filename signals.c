/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <signal.h>
#include <stdio.h>
#include <stdlib.h> /* for NULL */
#include <unistd.h>
#include <sys/time.h> /* for NULL */
#include "main.h"

/* Some externs refer to this variable */
static sigset_t normal_sigmask;
static struct sigaction original_int, original_term, original_hup;

/* as extern in execute.c */
volatile sig_atomic_t signals_child_pid;

void ignore_sigpipe()
{
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    sigprocmask(SIG_BLOCK, &set, &normal_sigmask);
    sigaction(SIGINT, NULL, &original_int);
    sigaction(SIGTERM, NULL, &original_term);
    sigaction(SIGHUP, NULL, &original_hup);
}

void restore_sigmask()
{
    sigaction(SIGINT, &original_int, NULL);
    sigaction(SIGTERM, &original_term, NULL);
    sigaction(SIGHUP, &original_hup, NULL);
    sigprocmask(SIG_SETMASK, &normal_sigmask, NULL);
}

void sigint_handler(int s)
{
    if (signals_child_pid)
    {
        kill(-signals_child_pid, s);
    } else
    {
        /* ts client killed by SIGINT */
        _exit(128 + s);
    }
}

void block_sigint()
{
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGHUP);
    /* ignore_sigpipe() will always be called first, and
     * only that sets the normal_sigmask. */
    sigprocmask(SIG_BLOCK, &set, 0);
}

void unblock_sigint_and_install_handler()
{
    sigset_t set;
    struct sigaction act;

    /* Install the handler */
    act.sa_handler = sigint_handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);
    sigaction(SIGTERM, &act, 0);
    sigaction(SIGHUP, &act, 0);

    /* Unblock the signal */
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGHUP);
    /* ignore_sigpipe() will always be called first, and
     * only that sets the normal_sigmask. */
    sigprocmask(SIG_UNBLOCK, &set, 0);
}
