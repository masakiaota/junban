/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <signal.h>
#include <errno.h>
#include <math.h>
#include <poll.h>
#include <limits.h>
#include "main.h"

static void c_end_of_job(const struct Result *res);

char *build_command_string()
{
    int size;
    int i;
    int num;
    char **array;
    char *commandstring;
    
    size = 0;
    num = command_line.command.num;
    array = command_line.command.array;

    /* Count bytes needed */
    for (i = 0; i < num; ++i)
    {
        /* The '1' is for spaces, and at the last i,
         * for the null character */
        size = size + strlen(array[i]) + 1;
    }

    /* Alloc */
    commandstring = (char *) malloc(size);
    if(commandstring == NULL)
        error("Error in malloc for commandstring");

    /* Build the command */
    strcpy(commandstring, array[0]);
    for (i = 1; i < num; ++i)
    {
        strcat(commandstring, " ");
        strcat(commandstring, array[i]);
    }

    return commandstring;
}

void c_new_job()
{
    struct msg m = {0};
    char *new_command;

    m.type = NEWJOB;

    new_command = build_command_string();

    /* Reserved wire fields stay zero to retain the fixed message frame. */
    m.u.newjob.command_size = strlen(new_command) + 1;
    if (command_line.label)
        m.u.newjob.label_size = strlen(command_line.label) + 1; /* add null */
    else
        m.u.newjob.label_size = 0;
    m.u.newjob.num_slots = command_line.num_slots;

    /* Send the message */
    send_msg(server_socket, &m);

    /* Keep the fixed message frame compatible for version negotiation.
     * This phase-2 payload is sent only after a successful version check. */
    send_bytes(server_socket, (const char *)&command_line.estimate,
               sizeof(command_line.estimate));

    /* Send the command */
    send_bytes(server_socket, new_command, m.u.newjob.command_size);

    /* Send the label */
    send_bytes(server_socket, command_line.label, m.u.newjob.label_size);

    free(new_command);
}

int c_wait_newjob_ok()
{
    struct msg m;
    int res;

    res = recv_msg(server_socket, &m);
    if(res != sizeof(m))
        error("queue disconnected while submitting");
    if(m.type == NEWJOB_NOK)
    {
        error("queue full");
    }
    if(m.type != NEWJOB_OK)
        error("Error getting the newjob_ok");

    return m.u.jobid;
}

int c_wait_server_commands()
{
    struct msg m;
    int res;
    int waited = 0;
    double enqueued = monotime(), next_notice = 0, interval = 300;
    const char *setting = getenv("JUNBAN_NOTIFY_SEC");
    if (setting) {
        char *end;
        errno = 0;
        interval = strtod(setting, &end);
        if (errno || !*setting || *end || !isfinite(interval) || interval <= 0 ||
            interval > 2147483647)
            error("JUNBAN_NOTIFY_SEC must be a positive number");
    }

    while (1)
    {
        if (waited) {
            double delay = next_notice - monotime();
            struct pollfd connection = {server_socket, POLLIN, 0};
            if (delay < 0) delay = 0;
            int timeout = delay >= INT_MAX / 1000.0 ? INT_MAX : (int)(delay * 1000 + 0.999);
            res = poll(&connection, 1, timeout);
            if (res < 0) {
                if (errno == EINTR) continue;
                error("cannot wait for queue notification");
            }
            if (!res) {
                memset(&m, 0, sizeof(m));
                m.type = WAIT_NOTICE;
                send_msg(server_socket, &m);
                next_notice = monotime() + interval;
                continue;
            }
        }
        res = recv_msg(server_socket, &m);
        if(res == -1)
            error("queue disconnected while waiting");

        if (res == 0)
            error("queue disconnected while waiting");
        if(res != sizeof(m))
            error("Error in wait_server_commands");
        if (m.type == JOB_REMOVED)
            error("job removed from queue");
        if (m.type == LIST_LINE) {
            char *line;
            if (m.u.size < 1 || m.u.size > 4096) error("invalid queue notification");
            line = malloc((size_t)m.u.size);
            if (!line) error("out of memory");
            if (recv_bytes(server_socket, line, m.u.size) != m.u.size)
                error("queue disconnected while notifying");
            line[m.u.size - 1] = 0;
            fprintf(stderr, "junban: %s", line);
            free(line);
            continue;
        }
        if (m.type == NOTICE_END) {
            waited = 1;
            next_notice = monotime() + interval;
            continue;
        }
        if (m.type == RUNJOB)
        {
            struct Result res = {0};
            if (waited) {
                char duration[48];
                format_duration(duration, sizeof(duration),
                                (unsigned long long)(monotime() - enqueued));
                fprintf(stderr, "junban: id=%d started after %s\n", command_line.jobid, duration);
            }
            run_job(&res);
            c_end_of_job(&res);
            return res.errorlevel;
        }
    }
    return -1;
}

/* Exits if wrong */
void c_check_version()
{
    struct msg m;
    int res;

    m.type = GET_VERSION;
    send_msg(server_socket, &m);

    res = recv_msg(server_socket, &m);
    if(res != sizeof(m))
        error("queue disconnected during handshake");
    if (m.type == NEWJOB_NOK)
        error("queue full");
    if (m.type != VERSION || m.u.version != PROTOCOL_VERSION)
    {
        error("Wrong server version. Received %i, expecting %i",
            m.u.version, PROTOCOL_VERSION);
    }

}

void c_send_runjob_ok(int pid)
{
    struct msg m = {0};
    m.type = RUNJOB_OK;
    m.u.output.pid = pid;
    send_msg(server_socket, &m);
}

static void c_end_of_job(const struct Result *res)
{
    struct msg m;

    m.type = ENDJOB;
    m.u.result = *res; /* struct copy */

    send_msg(server_socket, &m);
}

void c_shutdown_server()
{
    struct msg m;

    m.type = KILL_SERVER;
    send_msg(server_socket, &m);
}

void c_kill_job()
{
    struct msg m = {0};
    m.type = KILL_JOB;
    m.u.jobid = command_line.jobid;
    send_msg(server_socket, &m);
    if (recv_msg(server_socket, &m) != sizeof(m))
        error("queue disconnected during kill");
    if (m.type != KILL_JOB_OK)
        error("unknown job ID");
}

void c_get_max_slots()
{
    struct msg m;
    int res;

    /* Send the request */
    m.type = GET_MAX_SLOTS;
    m.u.max_slots = command_line.max_slots;
    send_msg(server_socket, &m);

    /* Receive the answer */
    res = recv_msg(server_socket, &m);
    if(res != sizeof(m))
        error("cannot read queue CPU total");
    switch(m.type)
    {
        case GET_MAX_SLOTS_OK:
            command_line.max_slots = m.u.max_slots;
            return;
        default:
            warning("Wrong internal message in get_max_slots");
    }
}

