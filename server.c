/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2013  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#ifdef linux
  #include <sys/time.h>
#endif
#include <sys/resource.h>
#include <sys/un.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <signal.h>
#include <fcntl.h>
#include <libgen.h>

#include <stdio.h>

#include "main.h"

enum
{
    MAXCONN=1000
};

enum Break
{
    BREAK,
    NOBREAK,
    CLOSE
};

/* Prototypes */
static void server_loop(int ls);
static enum Break
    client_read(int index);
static void end_server(int ls);
static void s_newjob_ok(int index);
static void s_newjob_nok(int index);
static void s_runjob(int jobid, int index);
static void clean_after_client_disappeared(int socket, int index);

struct Client_conn
{
    int socket;
    int hasjob;
    int jobid;
    int notice_pending;
};

/* Globals */
static struct Client_conn client_cs[MAXCONN];
static int nconnections;
static char *path;
static int max_descriptors;

/* in jobs.c */
extern int max_jobs;
extern int max_slots;

static void s_send_version(int s)
{
    struct msg m;

    m.type = VERSION;
    m.u.version = PROTOCOL_VERSION;

    send_msg(s, &m);
}

static void sigterm_handler(int n)
{
    (void)n;
    /* kill/waitpid in this cleanup are async-signal-safe. Stop even if a
     * client has left the server blocked in a partial protocol read. */
    stop_sleep_prevention();
    unlink(path);
    _exit(0);
}

static void install_sigterm_handler()
{
  struct sigaction act;

  act.sa_handler = sigterm_handler;
  /* Reset the mask */
  memset(&act.sa_mask,0,sizeof(act.sa_mask));
  act.sa_flags = 0;

  sigaction(SIGTERM, &act, NULL);
}

static int get_max_descriptors()
{
    const int MARGIN = 8; /* stdio, lock, listener, startup pipe, rejection FD */
    int max;
    struct rlimit rlim;
    int res;

    max = MAXCONN;

    if (max > FD_SETSIZE - MARGIN)
        max = FD_SETSIZE - MARGIN;

    /* I'd like to use OPEN_MAX or NR_OPEN, but I don't know if any
     * of them is POSIX compliant */

    res = getrlimit(RLIMIT_NOFILE, &rlim);
    if (res != 0)
        warning("getrlimit for open files");
    else
    {
        if ((rlim_t)(max + MARGIN) > rlim.rlim_cur)
            max = rlim.rlim_cur - MARGIN;
    }

    if (max < 6)
        error("Too few opened descriptors available");

    return max;
}

void server_main(int notify_fd, char *_path, int ls)
{
    process_type = SERVER;
    max_descriptors = get_max_descriptors();

    /* Arbitrary limit, that will block the enqueuing, but should allow space
     * for usual ts queries */
    max_jobs = max_descriptors - 5;

    path = _path;

    /* Keep the startup cwd: JUNBAN_SOCKET may be relative. */
    if (ls >= FD_SETSIZE) error("too many inherited file descriptors");

    nconnections = 0;

    install_sigterm_handler();
    atexit(stop_sleep_prevention);

    s_set_max_slots(detect_slots());

    notify_parent(notify_fd);

    server_loop(ls);
}

static int get_conn_of_jobid(int jobid)
{
    int i;
    for(i=0; i< nconnections; ++i)
        if (client_cs[i].hasjob && client_cs[i].jobid == jobid)
            return i;
    return -1;
}

static void server_loop(int ls)
{
    fd_set readset;
    int i;
    int maxfd;
    int keep_loop = 1;
    int newjob;

    while (keep_loop)
    {
        struct timeval interval = {0, 100000};
        FD_ZERO(&readset);
        maxfd = 0;
        /* Always accept, including at capacity, to reject explicitly. */
        {
            FD_SET(ls,&readset);
            maxfd = ls;
        }
        for(i=0; i< nconnections; ++i)
        {
            FD_SET(client_cs[i].socket, &readset);
            if (client_cs[i].socket > maxfd)
                maxfd = client_cs[i].socket;
        }
        if (select(maxfd + 1, &readset, NULL, NULL,
                   s_needs_poll() ? &interval : NULL) < 0) {
            if (errno == EINTR) continue;
            error("queue select failed");
        }
        if (FD_ISSET(ls,&readset))
        {
            int cs;
            cs = accept(ls, NULL, NULL);
            if (cs >= 0) {
                struct timeval timeout = {3, 0};
                if (setsockopt(cs, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0 ||
                    setsockopt(cs, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
                    close(cs);
                    continue;
                }
                if (nconnections >= max_descriptors || cs >= FD_SETSIZE) {
                    struct msg full = {0};
                    full.type = NEWJOB_NOK;
                    send_msg(cs, &full);
                    close(cs);
                } else {
                    client_cs[nconnections].hasjob = 0;
                    client_cs[nconnections].notice_pending = 0;
                    fcntl(cs, F_SETFD, FD_CLOEXEC);
                    client_cs[nconnections].socket = cs;
                    ++nconnections;
                }
            }
        }
        /* Descend because handling a request can remove a connection. */
        for(i=nconnections-1; i >= 0; --i)
            if (FD_ISSET(client_cs[i].socket, &readset))
            {
                enum Break b;
                b = client_read(i);
                /* Check if we should break */
                if (b == CLOSE)
                {
                    warning("Closing");
                    /* On unknown message, we close the client,
                       or it may hang waiting for an answer */
                    clean_after_client_disappeared(client_cs[i].socket, i);
                }
                else if (b == BREAK)
                {
                    keep_loop = 0;
                    break;
                }
            }
        s_poll_jobs();
        /* Acquire the idle-sleep assertion before granting any run permission. */
        prevent_sleep(keep_loop && s_has_jobs());
        /* This will return firstjob->jobid or -1 */
        while (keep_loop && (newjob = next_run_job()) != -1)
        {
            int conn;
            conn = get_conn_of_jobid(newjob);
            /* This next marks the firstjob state to RUNNING */
            s_mark_job_running(newjob);
            s_runjob(newjob, conn);


        }
        for (i = 0; i < nconnections; ++i) {
            if (client_cs[i].notice_pending) {
                s_wait_notice(client_cs[i].socket, client_cs[i].jobid);
                client_cs[i].notice_pending = 0;
            }
        }
    }

    end_server(ls);
}

static void end_server(int ls)
{
    stop_sleep_prevention();
    close(ls);
    unlink(path);
    /* This comes from the parent, in the fork after server_main.
     * This is the last use of path in this process.*/
    free(path); 
}

static void remove_connection(int index)
{
    int i;

    for(i=index; i<(nconnections-1); ++i)
    {
        memcpy(&client_cs[i], &client_cs[i+1], sizeof(client_cs[0]));
    }
    nconnections--;
}

static void
clean_after_client_disappeared(int socket, int index)
{
    /* Act as if the job ended. */
    int jobid = client_cs[index].jobid;
    if (client_cs[index].hasjob)
    {
        s_terminate_job(jobid);
        /* We don't want this connection to do anything
         * more related to the jobid, secially on remove_connection
         * when we receive the EOC. */
        client_cs[index].hasjob = 0;
    }

    close(socket);
    remove_connection(index);
}

static enum Break
    client_read(int index)
{
    struct msg m;
    int s;
    int res;

    s = client_cs[index].socket;

    /* Read the message */
    res = recv_msg(s, &m);
    if (res == -1)
    {
        warning("client recv failed");
        clean_after_client_disappeared(s, index);
        return NOBREAK;
    }
    else if (res == 0)
    {
        clean_after_client_disappeared(s, index);
        return NOBREAK;
    }

    /* Process message */
    switch(m.type)
    {
        case KILL_SERVER:
            return BREAK; /* break in the parent*/
            break;
        case SNAPSHOT:
            s_snapshot(s, m.u.size);
            break;
        case WAIT_NOTICE:
            if (client_cs[index].hasjob)
                s_wait_notice(s, client_cs[index].jobid);
            break;
        case NEWJOB:
            if (client_cs[index].hasjob || m.u.newjob.command_size < 1 ||
                m.u.newjob.num_slots < 0 ||
                m.u.newjob.num_slots > max_slots)
                return CLOSE;
            client_cs[index].jobid = s_newjob(s, &m);
            if (client_cs[index].jobid < 0) return CLOSE;
            client_cs[index].hasjob = 1;
            client_cs[index].notice_pending = 1;
            if (!job_is_holding_client(client_cs[index].jobid))
                s_newjob_ok(index);
            else
            {
                s_newjob_nok(index);
                clean_after_client_disappeared(s, index);
            }
            break;
        case RUNJOB_OK:
            {
                s_process_runjob_ok(client_cs[index].jobid, m.u.output.pid);
                m.type = RUNJOB_ACK;
                send_msg(s, &m);
            }
            break;
        case KILL_JOB:
            {
                int target = get_conn_of_jobid(m.u.jobid);
                int state = s_kill_job(m.u.jobid);
                struct msg reply = {0};
                reply.type = state ? KILL_JOB_OK : NEWJOB_NOK;
                send_msg(s, &reply);
                if (state == 2 && target >= 0) {
                    reply.type = JOB_REMOVED;
                    send_msg(client_cs[target].socket, &reply);
                    shutdown(client_cs[target].socket, SHUT_RDWR);
                }
            }
            break;
        case ENDJOB:
            s_end_job(&m.u.result, client_cs[index].jobid);
            /* We don't want this connection to do anything
             * more related to the jobid, secially on remove_connection
             * when we receive the EOC. */
            client_cs[index].hasjob = 0;
            break;
        case GET_MAX_SLOTS:
            s_get_max_slots(s);
            break;
        case GET_VERSION:
            s_send_version(s);
            break;
        default:
            /* Command not supported */
            /* On unknown message, we close the client,
               or it may hang waiting for an answer */
            warning("Unknown message: %i", m.type);
            return CLOSE;
    }

    return NOBREAK; /* normal */
}

static void s_runjob(int jobid, int index)
{
    int s;
    
    if (!client_cs[index].hasjob)
        error("Run job of the client %i which doesn't have any job", index);

    s = client_cs[index].socket;

    s_send_runjob(s, jobid);
}

static void s_newjob_ok(int index)
{
    int s;
    struct msg m;
    
    if (!client_cs[index].hasjob)
        error("Run job of the client %i which doesn't have any job", index);

    s = client_cs[index].socket;

    m.type = NEWJOB_OK;
    m.u.jobid = client_cs[index].jobid;

    send_msg(s, &m);
}

static void s_newjob_nok(int index)
{
    int s;
    struct msg m;
    
    if (!client_cs[index].hasjob)
        error("Run job of the client %i which doesn't have any job", index);

    s = client_cs[index].socket;

    m.type = NEWJOB_NOK;

    send_msg(s, &m);
}

static void dump_conn_struct(FILE *out, const struct Client_conn *p)
{
    fprintf(out, "  new_conn\n");
    fprintf(out, "    socket %i\n", p->socket);
    fprintf(out, "    hasjob \"%i\"\n", p->hasjob);
    fprintf(out, "    jobid %i\n", p->jobid);
}

void dump_conns_struct(FILE *out)
{
    int i;

    fprintf(out, "New_conns");

    for(i=0; i < nconnections; ++i)
    {
        dump_conn_struct(out, &client_cs[i]);
    }
}
