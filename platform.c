/* Modified for junban in 2026 by junban contributors. */
/* macOS idle-sleep prevention; Linux intentionally does nothing.
 * GPL-2.0, see COPYING. */
#include <stdio.h>
#include <sys/time.h>
#include "main.h"
#ifdef __APPLE__
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

static pid_t inhibitor;
static int attempted;

void stop_sleep_prevention(void)
{
    if (inhibitor > 0) {
        int status;
        kill(inhibitor, SIGTERM);
        while (waitpid(inhibitor, &status, 0) < 0 && errno == EINTR) {}
        inhibitor = 0;
    }
    attempted = 0;
}

void prevent_sleep(int active)
{
    int status;
    char parent[32];
    sigset_t block, previous;
    if (!active) { stop_sleep_prevention(); return; }
    if (inhibitor > 0 && waitpid(inhibitor, &status, WNOHANG) == inhibitor)
        inhibitor = 0;
    if (attempted) return;
    attempted = 1;
    if (access("/usr/bin/caffeinate", X_OK) < 0) return;
    snprintf(parent, sizeof(parent), "%ld", (long)getpid());
    sigemptyset(&block);
    sigaddset(&block, SIGTERM);
    sigprocmask(SIG_BLOCK, &block, &previous);
    inhibitor = fork();
    if (inhibitor == 0) {
        sigset_t empty;
        signal(SIGTERM, SIG_DFL);
        signal(SIGINT, SIG_DFL);
        signal(SIGHUP, SIG_DFL);
        sigemptyset(&empty);
        sigprocmask(SIG_SETMASK, &empty, NULL);
        /* All server-only descriptors have FD_CLOEXEC. -w also cleans up
         * after SIGKILL, when the server cannot run its normal exit path. */
        execl("/usr/bin/caffeinate", "caffeinate", "-i", "-w", parent, (char *)NULL);
        _exit(0);
    }
    sigprocmask(SIG_SETMASK, &previous, NULL);
    if (inhibitor < 0) inhibitor = 0;
}
#else
void stop_sleep_prevention(void) {}
void prevent_sleep(int active) { (void)active; }
#endif
