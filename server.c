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
#include <sys/stat.h>
#include <sys/wait.h>
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
static int lock_fd = -1;
/* The junban the server was started as, and that file when last checked.
 * When it is replaced, the server copies the new one to staged, checks
 * that copy, and execs it, keeping its state. */
static char *self_path;
static struct stat self_stat;
static char *staged;

/* in jobs.c */
extern int max_jobs;
extern int max_slots;
extern int max_gpus;

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

/* Record where this junban was run from, as an absolute path that still
 * follows symlinks, so a server notices when an upgrade replaces it. */
void set_self_path(const char *argv0)
{
    char cwd[PATH_MAX], *found = NULL;
    const char *dirs = getenv("PATH");
    if (strchr(argv0, '/')) {
        if (argv0[0] == '/') found = strdup(argv0);
        else if (getcwd(cwd, sizeof(cwd)) && (found = malloc(strlen(cwd) + strlen(argv0) + 2)))
            sprintf(found, "%s/%s", cwd, argv0);
    } else if (dirs) {
        while (!found) {
            size_t length = strcspn(dirs, ":");
            /* An empty PATH entry is the current directory. */
            const char *dir = length ? dirs : ".";
            int dir_length = length ? (int)length : 1;
            char *candidate = malloc((size_t)dir_length + strlen(argv0) + 2);
            if (!candidate) break;
            sprintf(candidate, "%.*s/%s", dir_length, dir, argv0);
            if (access(candidate, X_OK) == 0) {
                if (candidate[0] == '/') found = candidate;
                else {
                    set_self_path(candidate);
                    free(candidate);
                    return;
                }
            } else free(candidate);
            if (!dirs[length]) break;
            dirs += length + 1;
        }
    }
    free(self_path);
    self_path = found;
}

static void inherit_fd(int fd, int inherit)
{
    int flags = fcntl(fd, F_GETFD);
    if (flags >= 0)
        fcntl(fd, F_SETFD, inherit ? flags & ~FD_CLOEXEC : flags | FD_CLOEXEC);
}

static void inherit_server_fds(int ls, int inherit)
{
    inherit_fd(ls, inherit);
    inherit_fd(lock_fd, inherit);
    for (int i = 0; i < nconnections; ++i) inherit_fd(client_cs[i].socket, inherit);
}

/* If the junban at self_path has been replaced since the server last
 * looked, copy it to a private file beside the socket and return that
 * file's name, or NULL. The server checks and execs this copy, so it runs
 * the very image it checked even if the installed one changes again.
 * Each replacement is staged once. */
static char *stage_self(void)
{
    char *name, buffer[65536];
    struct stat st;
    ssize_t n = 0;
    int in, out, ok;
    if (!self_path || (in = open(self_path, O_RDONLY | O_CLOEXEC)) < 0) return NULL;
    if (fstat(in, &st) < 0 ||
        (st.st_dev == self_stat.st_dev && st.st_ino == self_stat.st_ino &&
         st.st_size == self_stat.st_size && st.st_mtime == self_stat.st_mtime) ||
        !(name = malloc(strlen(path) + sizeof(".bin.XXXXXX")))) {
        close(in);
        return NULL;
    }
    self_stat = st;
    sprintf(name, "%s.bin.XXXXXX", path);
    if ((out = mkstemp(name)) < 0) {
        close(in);
        free(name);
        return NULL;
    }
    ok = fchmod(out, 0700) == 0;
    while (ok && (n = read(in, buffer, sizeof(buffer))) > 0)
        ok = write(out, buffer, (size_t)n) == n;
    ok = ok && n == 0;
    close(in);
    if (close(out) < 0) ok = 0;
    if (!ok) {
        unlink(name);
        free(name);
        return NULL;
    }
    return name;
}

static void drop_staged(void)
{
    if (!staged) return;
    unlink(staged);
    free(staged);
    staged = NULL;
}

/* Ask the staged junban whether it speaks this protocol and reads this
 * state layout. Clients stay connected through the exec, so both must
 * match. */
static int staged_compatible(void)
{
    char expected[64], got[64];
    size_t length = 0;
    int p[2], status;
    double deadline = monotime() + 2;
    pid_t pid;
    snprintf(expected, sizeof(expected), "junban-live %d %d\n",
             PROTOCOL_VERSION, LIVE_STATE_VERSION);
    if (pipe(p) < 0) return 0;
    pid = fork();
    if (pid < 0) {
        close(p[0]);
        close(p[1]);
        return 0;
    }
    if (pid == 0) {
        dup2(p[1], STDOUT_FILENO);
        close(p[0]);
        close(p[1]);
        execl(staged, staged, "--live-check", (char *)NULL);
        _exit(127);
    }
    close(p[1]);
    while (length < sizeof(got)) {
        fd_set set;
        double left = deadline - monotime();
        struct timeval wait = {0, 0};
        ssize_t n;
        if (left <= 0) break;
        wait.tv_sec = (time_t)left;
        wait.tv_usec = (suseconds_t)((left - (double)wait.tv_sec) * 1e6);
        FD_ZERO(&set);
        FD_SET(p[0], &set);
        if (select(p[0] + 1, &set, NULL, NULL, &wait) <= 0) {
            if (errno == EINTR) continue;
            break;
        }
        n = read(p[0], got + length, sizeof(got) - length);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        length += (size_t)n;
    }
    close(p[0]);
    kill(pid, SIGKILL);
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    return length == strlen(expected) && !memcmp(got, expected, length);
}

/* Hand the queue to the staged junban: save the state to an unlinked
 * file, keep the listening socket, the lock and every client connection
 * open, and exec. The process ID stays, so clients and the lock see the
 * same server. Returns only if that failed; the server then goes on. */
static void live_upgrade(int ls)
{
    char *name, fdtext[16];
    FILE *f;
    int fd;
    if (!(name = malloc(strlen(path) + sizeof(".live.XXXXXX")))) return;
    sprintf(name, "%s.live.XXXXXX", path);
    fd = mkstemp(name);
    if (fd >= 0) unlink(name);
    free(name);
    if (fd < 0) return;
    if (!(f = fdopen(fd, "w+"))) {
        close(fd);
        return;
    }
    live_put_string(f, "junban-live");
    {
        int version = LIVE_STATE_VERSION, protocol = PROTOCOL_VERSION;
        live_put(f, &version, sizeof(version));
        live_put(f, &protocol, sizeof(protocol));
    }
    live_put(f, &ls, sizeof(ls));
    live_put(f, &lock_fd, sizeof(lock_fd));
    live_put_string(f, path);
    live_put_string(f, self_path);
    live_put_string(f, staged);
    {
        long long id[4] = {(long long)self_stat.st_dev, (long long)self_stat.st_ino,
                           (long long)self_stat.st_size, (long long)self_stat.st_mtime};
        live_put(f, id, sizeof(id));
    }
    live_put(f, &nconnections, sizeof(nconnections));
    for (int i = 0; i < nconnections; ++i) {
        live_put(f, &client_cs[i].socket, sizeof(client_cs[i].socket));
        live_put(f, &client_cs[i].hasjob, sizeof(client_cs[i].hasjob));
        live_put(f, &client_cs[i].jobid, sizeof(client_cs[i].jobid));
        live_put(f, &client_cs[i].notice_pending, sizeof(client_cs[i].notice_pending));
    }
    s_save_jobs(f);
    if (fflush(f) != 0 || ferror(f)) {
        warning("cannot save the queue for a live upgrade");
        fclose(f);
        return;
    }
    stop_sleep_prevention();
    inherit_server_fds(ls, 1);
    inherit_fd(fd, 1);
    snprintf(fdtext, sizeof(fdtext), "%d", fd);
    execl(staged, staged, "--live-resume", fdtext, (char *)NULL);
    warning("live upgrade failed: %s", strerror(errno));
    inherit_server_fds(ls, 0);
    fclose(f);
}

void server_main(int notify_fd, char *_path, int ls, int lockfd)
{
    process_type = SERVER;
    lock_fd = lockfd;
    if (self_path && stat(self_path, &self_stat) < 0) {
        free(self_path);
        self_path = NULL;
    }
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

/* The new junban after a live upgrade: read the state the old server saved
 * and carry on with its socket, lock and clients. */
void server_resume(int fd)
{
    FILE *f;
    char *magic;
    int version, protocol, ls;
    process_type = SERVER;
    if (!(f = fdopen(fd, "r"))) error("cannot read live state");
    rewind(f);
    magic = live_get_string(f);
    live_get(f, &version, sizeof(version));
    live_get(f, &protocol, sizeof(protocol));
    if (!magic || strcmp(magic, "junban-live") || version != LIVE_STATE_VERSION ||
        protocol != PROTOCOL_VERSION)
        error("cannot read live state of another version");
    free(magic);
    live_get(f, &ls, sizeof(ls));
    live_get(f, &lock_fd, sizeof(lock_fd));
    path = live_get_string(f);
    self_path = live_get_string(f);
    staged = live_get_string(f);
    {
        long long id[4];
        live_get(f, id, sizeof(id));
        self_stat.st_dev = (dev_t)id[0];
        self_stat.st_ino = (ino_t)id[1];
        self_stat.st_size = (off_t)id[2];
        self_stat.st_mtime = (time_t)id[3];
    }
    live_get(f, &nconnections, sizeof(nconnections));
    if (!path || nconnections < 0 || nconnections > MAXCONN)
        error("cannot read live state");
    for (int i = 0; i < nconnections; ++i) {
        live_get(f, &client_cs[i].socket, sizeof(client_cs[i].socket));
        live_get(f, &client_cs[i].hasjob, sizeof(client_cs[i].hasjob));
        live_get(f, &client_cs[i].jobid, sizeof(client_cs[i].jobid));
        live_get(f, &client_cs[i].notice_pending, sizeof(client_cs[i].notice_pending));
    }
    s_load_jobs(f);
    fclose(f);
    inherit_server_fds(ls, 0);
    /* This process runs from the staged copy; the installed file stays. */
    drop_staged();
    max_descriptors = get_max_descriptors();
    max_jobs = max_descriptors - 5;
    install_sigterm_handler();
    atexit(stop_sleep_prevention);
    /* The old server stopped sleep prevention just before the exec. */
    prevent_sleep(s_has_jobs());
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
        /* Between requests, so no client message is half read. */
        if (staged) {
            live_upgrade(ls);
            drop_staged();
            prevent_sleep(s_has_jobs());
        }
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
                /* A new client may be the first run of an upgraded junban. */
                if (!staged && (staged = stage_self()) && !staged_compatible())
                    drop_staged();
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
        case JOB_INFO:
            s_job_info(s, m.u.jobid);
            break;
        case WAIT_NOTICE:
            if (client_cs[index].hasjob)
                s_wait_notice(s, client_cs[index].jobid);
            break;
        case NEWJOB:
            if (client_cs[index].hasjob || m.u.newjob.command_size < 1 ||
                m.u.newjob.num_slots < 0 ||
                m.u.newjob.label_size < 0 || m.u.newjob.label_size > 4096 ||
                m.u.newjob.tag_size < 0 || m.u.newjob.tag_size > 64 ||
                m.u.newjob.log_size < 0 || m.u.newjob.log_size > 4096 ||
                m.u.newjob.num_slots > max_slots ||
                m.u.newjob.num_gpus < 0 || m.u.newjob.num_gpus > max_gpus)
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
