/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009 Lluís Batlle i Rossell
    Please find the license in the provided COPYING file.
*/
/* flock and online-CPU discovery are platform extensions to POSIX. */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#elif defined(__linux__)
#define _DEFAULT_SOURCE
#endif
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <fcntl.h>
#include <pwd.h>
#include <limits.h>
#include <time.h>
#include "main.h"

static char *socket_path;

/* Only this function chooses the queue location. Override paths may be
 * relative (useful with deeply nested test directories on macOS). */
void create_socket_path(char **path)
{
    const char *override = getenv("JUNBAN_SOCKET");
    const char *home;
    struct passwd *pw;
    char *dir, *p;
    struct stat st;
    if (override) {
        *path = strdup(override);
    } else {
        pw = getpwuid(getuid());
        home = pw ? pw->pw_dir : NULL;
        if (!home) error("cannot determine home directory");
#ifdef __APPLE__
        dir = malloc(strlen(home) + sizeof("/Library/Caches/junban"));
        sprintf(dir, "%s/Library/Caches/junban", home);
#else
        const char *runtime = getenv("XDG_RUNTIME_DIR");
        const char *base = runtime ? runtime : home;
        const char *suffix = runtime ? "/junban" : "/.cache/junban";
        dir = malloc(strlen(base) + strlen(suffix) + 1);
        sprintf(dir, "%s%s", base, suffix);
#endif
        /* Create missing parents, without changing existing parents' modes. */
        for (p = dir + 1; ; ++p) {
            if (*p == '/' || *p == '\0') {
                char saved = *p;
                *p = '\0';
                if (mkdir(dir, 0700) < 0 && errno != EEXIST)
                    error("cannot create queue directory: %s", strerror(errno));
                *p = saved;
                if (!saved) break;
            }
        }
        if (lstat(dir, &st) < 0 || !S_ISDIR(st.st_mode) || st.st_uid != getuid())
            error("queue directory must be owned by this user");
        if (chmod(dir, 0700) < 0)
            error("cannot secure queue directory: %s", strerror(errno));
        *path = malloc(strlen(dir) + sizeof("/queue.sock"));
        sprintf(*path, "%s/queue.sock", dir);
        free(dir);
    }
    if (!*path) error("out of memory");
    if (!**path || strlen(*path) >= sizeof(((struct sockaddr_un *)0)->sun_path))
        error("socket path is too long or empty");
}

int detect_slots(void)
{
    const char *s = getenv("JUNBAN_SLOTS");
    long n = 0;
    if (s) {
        if (!*s) error("invalid JUNBAN_SLOTS");
        for (; *s; ++s) {
            if (*s < '0' || *s > '9' || n > (INT_MAX - (*s - '0')) / 10)
                error("invalid JUNBAN_SLOTS");
            n = n * 10 + (*s - '0');
        }
    } else n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1 || n > INT_MAX) error("cannot determine CPU count");
    return (int)n;
}

int try_connect(int s)
{
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, socket_path);
    return connect(s, (struct sockaddr *)&addr, sizeof(addr));
}

void wait_server_up(int fd)
{
    char a;
    ssize_t n;
    do { n = read(fd, &a, 1); } while (n < 0 && errno == EINTR);
    close(fd);
    if (n != 1) error("server failed to start");
}

void notify_parent(int fd)
{
    char a = 'a';
    if (write(fd, &a, 1) != 1) _exit(125);
    close(fd);
}

static int connect_queue(void)
{
    int s = socket(AF_UNIX, SOCK_STREAM, 0), saved;
    if (s < 0) error("cannot create queue socket: %s", strerror(errno));
    fcntl(s, F_SETFD, FD_CLOEXEC);
    fcntl(s, F_SETFL, O_NONBLOCK);
    if (try_connect(s) == 0) {
        fcntl(s, F_SETFL, 0);
        return s;
    }
    saved = errno;
    close(s);
    errno = saved;
    return -1;
}

int ensure_server_up()
{
    char *lockpath;
    int lockfd, ls, p[2], pid, attempts = 0;
    struct stat st;
    struct sockaddr_un addr;
    struct timespec delay = {0, 20000000};
    create_socket_path(&socket_path);
    lockpath = malloc(strlen(socket_path) + sizeof(".lock"));
    sprintf(lockpath, "%s.lock", socket_path);

    for (;;) {
        server_socket = connect_queue();
        if (server_socket >= 0) break;
        if (errno != ENOENT && errno != ECONNREFUSED &&
            errno != EAGAIN && errno != EINPROGRESS)
            error("cannot reach queue: %s", strerror(errno));
        lockfd = open(lockpath, O_CREAT | O_RDWR | O_NOFOLLOW, 0600);
        if (lockfd < 0) error("cannot open queue lock: %s", strerror(errno));
        fcntl(lockfd, F_SETFD, FD_CLOEXEC);
        if (flock(lockfd, LOCK_EX | LOCK_NB) == 0) {
            /* Only the lock owner may remove a stale socket. Never unlink
             * the lock file: all future starters must lock this same inode. */
            if (lstat(socket_path, &st) == 0) {
                if (!S_ISSOCK(st.st_mode) || st.st_uid != getuid())
                    error("queue path is not an owned socket");
                if (unlink(socket_path) < 0)
                    error("cannot remove stale queue socket: %s", strerror(errno));
            } else if (errno != ENOENT)
                error("cannot inspect queue socket: %s", strerror(errno));
            /* Validate before forking so startup errors reach the caller. */
            s_set_max_slots(detect_slots());
            ls = socket(AF_UNIX, SOCK_STREAM, 0);
            if (ls < 0) error("cannot create queue socket: %s", strerror(errno));
            fcntl(ls, F_SETFD, FD_CLOEXEC);
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            strcpy(addr.sun_path, socket_path);
            if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) < 0)
                error("cannot create queue: %s", strerror(errno));
            if (chmod(socket_path, 0600) < 0 || listen(ls, 128) < 0)
                error("cannot listen on queue: %s", strerror(errno));
            if (pipe(p) < 0) error("cannot create startup pipe: %s", strerror(errno));
            pid = fork();
            if (pid < 0) error("cannot start server: %s", strerror(errno));
            if (pid == 0) {
                int fd, nullfd;
                struct rlimit lim;
                close(p[0]);
                /* Drop inherited caller descriptors, keeping only server FDs. */
                if (getrlimit(RLIMIT_NOFILE, &lim) < 0) _exit(125);
                for (fd = 3; (rlim_t)fd < lim.rlim_cur && fd < INT_MAX; ++fd)
                    if (fd != lockfd && fd != ls && fd != p[1]) close(fd);
                nullfd = open("/dev/null", O_RDWR);
                if (nullfd < 0) _exit(125);
                for (fd = 0; fd < 3; ++fd) dup2(nullfd, fd);
                if (nullfd > 2) close(nullfd);
                setsid();
                signal(SIGINT, SIG_IGN);
                signal(SIGHUP, SIG_IGN);
                if (ftruncate(lockfd, 0) == 0)
                    dprintf(lockfd, "%ld\n", (long)getpid());
                server_main(p[1], socket_path, ls);
                _exit(0); /* closes the lifetime lock */
            }
            close(p[1]);
            close(ls);
            close(lockfd);
            wait_server_up(p[0]);
        } else {
            int saved = errno;
            close(lockfd);
            if (saved != EWOULDBLOCK && saved != EAGAIN)
                error("cannot lock queue: %s", strerror(saved));
            if (++attempts == 100) error("queue full");
            nanosleep(&delay, NULL);
        }
    }
    free(lockpath);
    free(socket_path);
    return 1;
}
