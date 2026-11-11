/* Modified for junban in 2026 by junban contributors. */
/* junban visibility; GPL-2.0, see COPYING. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "main.h"

double monotime(void)
{
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) error("cannot read monotonic clock");
    return now.tv_sec + now.tv_nsec / 1000000000.0;
}

void format_duration(char *dst, size_t size, unsigned long long seconds)
{
    const unsigned long long units[] = {86400, 3600, 60, 1};
    const char suffix[] = "dhms";
    size_t used = 0;
    if (!seconds) { snprintf(dst, size, "0s"); return; }
    for (size_t i = 0; i < 4; ++i) {
        unsigned long long value = seconds / units[i];
        seconds %= units[i];
        if (!value) continue;
        if (used >= size) break;
        int count = snprintf(dst + used, size - used, "%llu%c", value, suffix[i]);
        if (count < 0) break;
        used += (size_t)count;
    }
}

static void safe_text(char *dst, size_t size, const char *src, int label)
{
    size_t used = 0;
    if (!src) src = "-";
    for (; *src && used + 1 < size; ++src) {
        unsigned char c = (unsigned char)*src;
        if (label && (c <= 32 || c >= 127 || c == '%')) {
            if (used + 3 >= size) break;
            snprintf(dst + used, size - used, "%%%02X", c);
            used += 3;
        } else dst[used++] = c < 32 || c == 127 ? ' ' : (char)c;
    }
    dst[used] = 0;
    if (*src && used >= 3) {
        used -= 3;
        while (used > 0 && ((unsigned char)dst[used] & 0xc0) == 0x80) --used;
        memcpy(dst + used, "...", 4);
    }
}

void job_view(const struct Job *job, struct Job_view *view)
{
    double now = monotime();
    memset(view, 0, sizeof(*view));
    view->jobid = job->jobid;
    view->state = job->state;
    view->cpus = job->num_slots;
    view->unspecified = job->cpus_unspecified;
    view->killed = job->kill_requested;
    view->client_lost = job->client_lost;
    view->status = job->result.errorlevel;
    view->signal = job->result.died_by_signal ? job->result.signal : 0;
    view->estimate = job->estimate;
    view->waited = (job->started_at ? job->started_at : now) - job->queued_at;
    if (job->started_at)
        view->elapsed = (job->finished_at ? job->finished_at : now) - job->started_at;
    safe_text(view->label, sizeof(view->label), job->label, 1);
    safe_text(view->command, sizeof(view->command), job->command, 0);
}

void view_cpus(const struct Job_view *view, char *dst, size_t size)
{
    if (view->unspecified) snprintf(dst, size, "all(%d)", view->cpus);
    else snprintf(dst, size, "%d", view->cpus);
}

void view_fields(const struct Job_view *view, char *dst, size_t size)
{
    char elapsed[48], estimate[48];
    format_duration(elapsed, sizeof(elapsed), (unsigned long long)
                    (view->state == QUEUED ? view->waited : view->elapsed));
    if (view->estimate) {
        format_duration(estimate, sizeof(estimate), view->estimate);
        snprintf(dst, size, "%s=%s est=%s%s",
                 view->state == QUEUED ? "waited" : "elapsed", elapsed, estimate,
                 view->state != QUEUED && view->elapsed > (double)view->estimate ? " over" : "");
    } else snprintf(dst, size, "%s=%s",
                    view->state == QUEUED ? "waited" : "elapsed", elapsed);
}

void view_status(const struct Job_view *view, char *dst, size_t size)
{
    if (view->killed) snprintf(dst, size, "killed");
    else if (view->client_lost) snprintf(dst, size, "client-lost");
    else if (view->signal) snprintf(dst, size, "signal=%d", view->signal);
    else snprintf(dst, size, "exit=%d", view->status);
}

int fetch_snapshot(struct Snapshot *snapshot)
{
    struct msg m = {0};
    size_t count = 0;
    m.type = SNAPSHOT;
    m.u.size = command_line.list_all;
    send_msg(server_socket, &m);
    snapshot->busy = snapshot->total = snapshot->running = snapshot->queued = 0;
    snapshot->count = 0;
    for (;;) {
        if (recv_msg(server_socket, &m) != (int)sizeof(m)) return 0;
        if (m.type == SNAPSHOT_END) { snapshot->total = m.u.max_slots; return 1; }
        if (m.type == NEWJOB_NOK) return 0;
        if (m.type != SNAPSHOT_ROW) return 0;
        struct Job_view *next = realloc(snapshot->jobs, (count + 1) * sizeof(*next));
        if (!next) error("out of memory");
        snapshot->jobs = next;
        if (recv_bytes(server_socket, (char *)&next[count], sizeof(*next)) != (int)sizeof(*next))
            return 0;
        struct Job_view *v = &next[count++];
        snapshot->count = count;
        if (!snapshot->total) snapshot->total = m.u.max_slots;
        if (v->state == RUNNING) { snapshot->busy += v->cpus; ++snapshot->running; }
        if (v->state == QUEUED) ++snapshot->queued;
    }
}

void print_snapshot(const struct Snapshot *snapshot)
{
    printf("cpus %d/%d running %d queued %d\n", snapshot->busy, snapshot->total,
           snapshot->running, snapshot->queued);
    for (size_t i = 0; i < snapshot->count; ++i) {
        const struct Job_view *v = &snapshot->jobs[i];
        if (!command_line.list_all && v->state != RUNNING && v->state != QUEUED) continue;
        char timing[128], cpus[32], status[48];
        view_cpus(v, cpus, sizeof(cpus));
        view_fields(v, timing, sizeof(timing));
        printf("%s id=%d dir=%s cpus=%s %s",
               v->state == RUNNING ? "running" : v->state == QUEUED ? "queued" : "done",
               v->jobid, v->label, cpus, timing);
        if (v->state != RUNNING && v->state != QUEUED) {
            view_status(v, status, sizeof(status));
            printf(" %s", status);
        }
        printf(" cmd=%s\n", v->command);
    }
}
