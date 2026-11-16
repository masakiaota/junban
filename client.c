/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
/* O_NOFOLLOW is a platform extension to POSIX.1-2008 here. */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#elif defined(__linux__)
#define _DEFAULT_SOURCE
#endif
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
#include <fcntl.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include "main.h"

static void c_end_of_job(const struct Result *res);
static double wait_deadline; /* Zero: junban wait has no timeout. */
static int wait_self = -1;    /* The calling job's ID inside a junban job. */
static _Noreturn void wait_request_failed(void);
static void wait_io_timeout(void);

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
    m.u.newjob.tag_size = command_line.tag ? strlen(command_line.tag) + 1 : 0;
    m.u.newjob.log_size = command_line.log_prefix ? strlen(command_line.log_prefix) + 1 : 0;
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

    /* Send the tag */
    send_bytes(server_socket, command_line.tag, m.u.newjob.tag_size);

    /* Send the log directory */
    send_bytes(server_socket, command_line.log_prefix, m.u.newjob.log_size);

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
            /* A detached job's log holds only the command's output. */
            char *line;
            if (m.u.size < 1 || m.u.size > 4096) error("invalid queue notification");
            line = malloc((size_t)m.u.size);
            if (!line) error("out of memory");
            if (recv_bytes(server_socket, line, m.u.size) != m.u.size)
                error("queue disconnected while notifying");
            line[m.u.size - 1] = 0;
            if (!command_line.detach) fprintf(stderr, "junban: %s", line);
            free(line);
            continue;
        }
        if (m.type == NOTICE_END) {
            if (command_line.detach) continue;
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
    if (res != sizeof(m) && wait_deadline) wait_request_failed();
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
    /* Stay connected until the server goes away: macOS refuses socket
     * options on a connection whose peer already closed, and the server
     * drops such a connection unread. */
    while (recv_msg(server_socket, &m) > 0) {}
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

/* 1 when the queue accepted the request to stop the job. */
static int kill_one(int jobid)
{
    struct msg m = {0};
    m.type = KILL_JOB;
    m.u.jobid = jobid;
    send_msg(server_socket, &m);
    if (recv_msg(server_socket, &m) != sizeof(m))
        error("queue disconnected during kill");
    /* A job that ended since the listing needs no stopping. */
    return m.type == KILL_JOB_OK;
}

/* Stop every running or queued job carrying the tag. Returns how many. */
int c_kill_tag(void)
{
    struct Snapshot snapshot = {0};
    int killed = 0, self = -1, self_matched = 0;
    const char *job = getenv("JUNBAN_JOB_ID");
    if (job) self = atoi(job);
    if (!fetch_snapshot(&snapshot)) error("queue disconnected while listing");
    for (size_t i = 0; i < snapshot.count; ++i) {
        const struct Job_view *v = &snapshot.jobs[i];
        if ((v->state != RUNNING && v->state != QUEUED) || !job_selected(v)) continue;
        /* Stopping the calling job also stops this client, so do that last. */
        if (v->jobid == self) self_matched = 1;
        else killed += kill_one(v->jobid);
    }
    free(snapshot.jobs);
    if (self_matched) killed += kill_one(self);
    return killed;
}

/* Detached logs live next to the queue socket, like its .lock file:
 * <socket>.logs/ID.log, so queues sharing a directory keep separate logs.
 * The socket may be in a shared directory, so the log directory must be
 * this user's own and private, and logs are opened without following
 * symlinks. */
char *c_log_prefix(void)
{
    char *path, *slash, *dir, *prefix;
    const char *name;
    struct stat st;
    create_socket_path(&path);
    slash = strrchr(path, '/');
    if (slash) {
        name = slash + 1;
        *slash = '\0';
        dir = realpath(slash == path ? "/" : path, NULL);
    } else {
        name = path;
        dir = realpath(".", NULL);
    }
    if (!dir) error("cannot find queue directory: %s", strerror(errno));
    prefix = malloc(strlen(dir) + strlen(name) + sizeof("//.logs/"));
    if (!prefix) error("out of memory");
    sprintf(prefix, "%s/%s.logs", strcmp(dir, "/") ? dir : "", name);
    free(dir);
    free(path);
    if (mkdir(prefix, 0700) < 0 && errno != EEXIST)
        error("cannot create log directory %s: %s", prefix, strerror(errno));
    if (lstat(prefix, &st) < 0 || !S_ISDIR(st.st_mode) || st.st_uid != getuid())
        error("log directory %s must be a directory owned by this user", prefix);
    if ((st.st_mode & 0777) != 0700 && chmod(prefix, 0700) < 0)
        error("cannot secure log directory %s: %s", prefix, strerror(errno));
    strcat(prefix, "/");
    return prefix;
}

/* Print the job ID and leave the job running in the background, its output
 * going to the log. The child keeps the queue connection and runs the job. */
void c_detach(void)
{
    char *log = malloc(strlen(command_line.log_prefix) + 16);
    int fd, nullfd, pid;
    struct rlimit lim;
    if (!log) error("out of memory");
    sprintf(log, "%s%d.log", command_line.log_prefix, command_line.jobid);
    /* main() keeps 0-2 open, so these land above them and survive dup2. */
    fd = open(log, O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW, 0600);
    if (fd < 0) error("cannot open job log %s: %s", log, strerror(errno));
    nullfd = open("/dev/null", O_RDONLY);
    if (nullfd < 0) error("cannot open /dev/null: %s", strerror(errno));
    fflush(NULL);
    pid = fork();
    if (pid < 0) error("cannot detach: %s", strerror(errno));
    if (pid > 0) {
        printf("%d\n", command_line.jobid);
        fprintf(stderr, "junban: id=%d log=%s\n", command_line.jobid, log);
        exit(fflush(stdout) ? 125 : 0);
    }
    setsid();
    dup2(nullfd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    /* Hold none of the caller's pipes, or a caller reading them to EOF
     * would wait for the job. */
    if (getrlimit(RLIMIT_NOFILE, &lim) == 0)
        for (int i = 3; (rlim_t)i < lim.rlim_cur && i < INT_MAX; ++i)
            if (i != server_socket) close(i);
    free(log);
}

/* A queue request of junban wait failed or ran into the timeout. */
static _Noreturn void wait_request_failed(void)
{
    if (wait_deadline && monotime() >= wait_deadline) {
        fputs("junban: timed out waiting for the queue\n", stderr);
        exit(124);
    }
    error("queue disconnected while waiting");
}

/* Bound each request by the time left, so a stalled server cannot hold
 * junban wait much past its timeout. */
static void wait_io_timeout(void)
{
    double left;
    struct timeval timeout;
    if (!wait_deadline) return;
    /* A short grace lets a responsive server answer the last check, and
     * keeps the timeout above zero, which would mean no timeout. */
    left = wait_deadline - monotime();
    left = (left > 0 ? left : 0) + 0.1;
    timeout.tv_sec = (time_t)left;
    timeout.tv_usec = (suseconds_t)((left - (double)timeout.tv_sec) * 1e6);
    setsockopt(server_socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(server_socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

/* Start the timeout of junban wait before the first queue request. */
void c_wait_prepare(void)
{
    const char *self = getenv("JUNBAN_JOB_ID");
    if (command_line.timeout) wait_deadline = monotime() + (double)command_line.timeout;
    if (self) wait_self = atoi(self);
    wait_io_timeout();
}

/* 1 with the job's view, 0 for an ID the queue does not know. */
static int job_info(int jobid, struct Job_view *view)
{
    struct msg m = {0};
    m.type = JOB_INFO;
    m.u.jobid = jobid;
    wait_io_timeout();
    send_msg(server_socket, &m);
    if (recv_msg(server_socket, &m) != sizeof(m)) wait_request_failed();
    if (m.type == NEWJOB_NOK) return 0;
    if (m.type != SNAPSHOT_ROW ||
        recv_bytes(server_socket, (char *)view, sizeof(*view)) != (int)sizeof(*view))
        wait_request_failed();
    return 1;
}

struct Target { int jobid, done; };

static int next_jobid(void)
{
    struct msg m = {0};
    m.type = JOB_INFO;
    m.u.jobid = -1;
    wait_io_timeout();
    send_msg(server_socket, &m);
    if (recv_msg(server_socket, &m) != sizeof(m) || m.type != JOB_INFO)
        wait_request_failed();
    return m.u.jobid;
}

static int tagged(const struct Job_view *view)
{
    for (int t = 0; t < command_line.num_tags; ++t)
        if (!strcmp(view->tag, command_line.tags[t])) return 1;
    return 0;
}

static void add_target(struct Target **targets, int *count, int *size, int jobid)
{
    for (int i = 0; i < *count; ++i)
        if ((*targets)[i].jobid == jobid) return;
    /* A job waiting for itself would never finish. */
    if (jobid == wait_self) error("job %d cannot wait for itself", jobid);
    if (*count == *size) {
        *size = *size * 2 + 8;
        struct Target *more = realloc(*targets, sizeof(**targets) * (size_t)*size);
        if (!more) error("out of memory");
        *targets = more;
    }
    (*targets)[*count].jobid = jobid;
    (*targets)[(*count)++].done = 0;
}

/* Wait until the given jobs have finished, and the tagged jobs that were
 * running or queued when the wait began or were queued since. Returns the
 * highest exit status among them, or 124 when the timeout passes first.
 * Unknown IDs are junban errors. */
int c_wait(void)
{
    struct Target *targets = NULL;
    int count = 0, size = 0, status = 0, scanned = 0;
    double deadline = wait_deadline;
    for (int i = 0; i < command_line.num_ids; ++i)
        add_target(&targets, &count, &size, command_line.ids[i]);
    if (command_line.num_tags) {
        /* The active jobs and the next-ID boundary come from one reply, so
         * no tagged job falls between them. */
        struct Snapshot snapshot = {0};
        wait_io_timeout();
        if (!fetch_snapshot(&snapshot)) wait_request_failed();
        scanned = snapshot.next_jobid;
        for (size_t i = 0; i < snapshot.count; ++i) {
            const struct Job_view *v = &snapshot.jobs[i];
            if ((v->state == RUNNING || v->state == QUEUED) && tagged(v))
                add_target(&targets, &count, &size, v->jobid);
        }
        free(snapshot.jobs);
    }
    for (;;) {
        int finished = 0;
        if (command_line.num_tags) {
            /* Check every job queued since, even one that already ended. */
            struct Job_view view;
            for (int next = next_jobid(); scanned < next; ++scanned)
                if (job_info(scanned, &view) && tagged(&view))
                    add_target(&targets, &count, &size, scanned);
        }
        for (int i = 0; i < count; ++i) {
            struct Job_view view;
            if (targets[i].done) { ++finished; continue; }
            if (!job_info(targets[i].jobid, &view))
                error("unknown job ID %d", targets[i].jobid);
            if (view.state == RUNNING || view.state == QUEUED) continue;
            targets[i].done = 1;
            ++finished;
            int code = view.status < 0 || view.status > 255 ? 125 : view.status;
            if (code > status) status = code;
            if (command_line.verbose) {
                char result[48], elapsed[48];
                view_status(&view, result, sizeof(result));
                format_duration(elapsed, sizeof(elapsed), (unsigned long long)view.elapsed);
                printf("finished id=%d %s elapsed=%s\n", view.jobid, result, elapsed);
                fflush(stdout);
            }
        }
        /* Return only if no tagged job was queued during this pass. */
        int pending = finished < count || (command_line.num_tags && next_jobid() != scanned);
        if (!pending) break;
        double now = monotime(), delay = finished < count ? 0.2 : 0;
        if (deadline && now >= deadline) {
            fprintf(stderr, "junban: timed out with %d of %d jobs unfinished\n",
                    count - finished, count);
            status = 124;
            break;
        }
        if (deadline && deadline - now < delay) delay = deadline - now;
        struct timespec pause = {0, (long)(delay * 1e9)};
        if (delay > 0) nanosleep(&pause, NULL);
    }
    free(targets);
    return status;
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

