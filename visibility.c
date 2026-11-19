/* Modified for junban in 2026 by junban contributors. */
/* junban visibility; GPL-2.0, see COPYING. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "main.h"
#include "unicode_tables.h"

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

/* Decode one UTF-8 character. Returns its length in bytes, or 0 when the
 * bytes are not valid UTF-8 (overlong forms and surrogates included). */
static int utf8_char(const unsigned char *s, unsigned *code)
{
    unsigned c = s[0];
    int n;
    if (c < 0x80) { *code = c; return 1; }
    if (c >= 0xc2 && c <= 0xdf) { n = 2; c &= 0x1f; }
    else if (c >= 0xe0 && c <= 0xef) { n = 3; c &= 0x0f; }
    else if (c >= 0xf0 && c <= 0xf4) { n = 4; c &= 0x07; }
    else return 0;
    /* A NUL fails the continuation test before any byte past it is read. */
    for (int i = 1; i < n; ++i) {
        if ((s[i] & 0xc0) != 0x80) return 0;
        c = c << 6 | (s[i] & 0x3f);
    }
    if ((n == 3 && c < 0x800) || (n == 4 && (c < 0x10000 || c > 0x10ffff)) ||
        (c >= 0xd800 && c <= 0xdfff))
        return 0;
    *code = c;
    return n;
}

static int in_table(const struct Unicode_range *table, size_t count, unsigned c)
{
    size_t low = 0, high = count;
    while (low < high) {
        size_t mid = (low + high) / 2;
        if (c < table[mid].first) high = mid;
        else if (c > table[mid].last) low = mid + 1;
        else return 1;
    }
    return 0;
}

#define IN_TABLE(table, c) in_table(table, sizeof(table) / sizeof(table[0]), c)

/* Terminal columns of a character, from the Unicode tables: 0 for
 * combining marks and format characters, 2 for East Asian wide and
 * fullwidth characters (emoji included), 1 otherwise. */
static int char_columns(unsigned c)
{
    if (IN_TABLE(zero_width_chars, c)) return 0;
    return IN_TABLE(wide_chars, c) ? 2 : 1;
}

/* Copy whole characters of src that fit in columns terminal columns, then
 * pad with spaces up to columns when pad is set. Invalid bytes show as '?'. */
void fit_columns(char *dst, size_t size, const char *src, int columns, int pad)
{
    const unsigned char *s = (const unsigned char *)src;
    size_t used = 0;
    int width = 0;
    while (*s) {
        unsigned code;
        int n = utf8_char(s, &code), w = n ? char_columns(code) : 1;
        if (!n) n = 1;
        if (width + w > columns || used + (size_t)n >= size) break;
        if (n == 1 && *s >= 0x80) dst[used] = '?';
        else memcpy(dst + used, s, (size_t)n);
        used += (size_t)n;
        width += w;
        s += n;
    }
    while (pad && width < columns && used + 1 < size) { dst[used++] = ' '; ++width; }
    dst[used] = 0;
}

/* Terminal columns of a string; invalid bytes count as one, like '?'. */
int text_columns(const char *src)
{
    const unsigned char *s = (const unsigned char *)src;
    int width = 0;
    while (*s) {
        unsigned code;
        int n = utf8_char(s, &code);
        width += n ? char_columns(code) : 1;
        s += n ? n : 1;
    }
    return width;
}

static void safe_text(char *dst, size_t size, const char *src, int label)
{
    size_t used = 0;
    if (!src) src = "-";
    for (; *src && used + 1 < size; ++src) {
        unsigned char c = (unsigned char)*src;
        unsigned code;
        int n;
        /* Labels keep printable UTF-8 such as Japanese directory names, but
         * not whitespace or line separators, which would split the field or
         * the line, nor invisible format characters. */
        if (label && c >= 0x80 && (n = utf8_char((const unsigned char *)src, &code)) &&
            code >= 0xa0 && !IN_TABLE(label_escaped_chars, code)) {
            if (used + (size_t)n >= size) break;
            memcpy(dst + used, src, (size_t)n);
            used += (size_t)n;
            src += n - 1;
            continue;
        }
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
    view->gpus = job->num_gpus;
    view->unspecified = job->cpus_unspecified;
    view->killed = job->kill_requested;
    view->client_lost = job->client_lost;
    view->status = job->result.errorlevel;
    view->signal = job->result.died_by_signal ? job->result.signal : 0;
    view->estimate = job->estimate;
    view->waited = (job->started_at ? job->started_at : now) - job->queued_at;
    if (job->started_at)
        view->elapsed = (job->finished_at ? job->finished_at : now) - job->started_at;
    if (job->finished_at) view->ended = (long long)time(NULL) - (long long)(now - job->finished_at);
    safe_text(view->label, sizeof(view->label), job->label, 1);
    if (job->tag) safe_text(view->tag, sizeof(view->tag), job->tag, 1);
    if (job->log) safe_text(view->log, sizeof(view->log), job->log, 1);
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
    snapshot->busy_gpus = snapshot->total_gpus = 0;
    snapshot->count = 0;
    for (;;) {
        if (recv_msg(server_socket, &m) != (int)sizeof(m)) return 0;
        if (m.type == SNAPSHOT_END) {
            snapshot->total = m.u.snapshot_end.max_slots;
            snapshot->next_jobid = m.u.snapshot_end.next_jobid;
            snapshot->total_gpus = m.u.snapshot_end.max_gpus;
            return 1;
        }
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
        if (v->state == RUNNING) {
            snapshot->busy += v->cpus;
            snapshot->busy_gpus += v->gpus;
            ++snapshot->running;
        }
        if (v->state == QUEUED) ++snapshot->queued;
    }
}

void print_summary(const struct Snapshot *snapshot)
{
    printf("cpus %d/%d gpus %d/%d running %d queued %d\n", snapshot->busy, snapshot->total,
           snapshot->busy_gpus, snapshot->total_gpus, snapshot->running, snapshot->queued);
}

int job_selected(const struct Job_view *view)
{
    int i, found;
    if (command_line.num_ids) {
        for (i = found = 0; i < command_line.num_ids && !found; ++i)
            found = view->jobid == command_line.ids[i];
        if (!found) return 0;
    }
    if (command_line.num_dirs) {
        /* Match the dir= field as printed, so encoded names work as shown. */
        char dir[sizeof(view->label)];
        for (i = found = 0; i < command_line.num_dirs && !found; ++i) {
            safe_text(dir, sizeof(dir), command_line.dirs[i], 1);
            found = !strcmp(view->label, dir) || !strcmp(view->label, command_line.dirs[i]);
        }
        if (!found) return 0;
    }
    if (command_line.num_tags) {
        for (i = found = 0; i < command_line.num_tags && !found; ++i)
            found = !strcmp(view->tag, command_line.tags[i]);
        if (!found) return 0;
    }
    return 1;
}

/* stdout carries job lines only, so an empty queue prints nothing.
 * Returns the number of lines printed. */
int print_snapshot(const struct Snapshot *snapshot)
{
    int printed = 0;
    for (size_t i = 0; i < snapshot->count; ++i) {
        const struct Job_view *v = &snapshot->jobs[i];
        if (!command_line.list_all && v->state != RUNNING && v->state != QUEUED) continue;
        if (!job_selected(v)) continue;
        char timing[128], cpus[32], status[48];
        view_cpus(v, cpus, sizeof(cpus));
        view_fields(v, timing, sizeof(timing));
        printf("%s id=%d dir=%s",
               v->state == RUNNING ? "running" : v->state == QUEUED ? "queued" : "done",
               v->jobid, v->label);
        if (v->tag[0]) printf(" tag=%s", v->tag);
        printf(" cpus=%s gpus=%d %s", cpus, v->gpus, timing);
        if (v->state != RUNNING && v->state != QUEUED) {
            view_status(v, status, sizeof(status));
            printf(" %s", status);
        }
        if (v->log[0]) printf(" log=%s", v->log);
        /* ls keeps its 80-byte command; watch has room for the wider one. */
        char command[81];
        safe_text(command, sizeof(command), v->command, 0);
        printf(" cmd=%s\n", command);
        ++printed;
    }
    return printed;
}
