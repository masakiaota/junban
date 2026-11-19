/* Modified for junban in 2026 by junban contributors. */
/* Plain ANSI queue monitor; GPL-2.0, see COPYING. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <sys/time.h>
#include <time.h>
#include "main.h"

static struct termios saved_terminal;
static int input_fd = -1, screen_active, modes_saved;
static volatile sig_atomic_t watch_stopping;
static int use_colour = 1, use_utf8 = 1;

/* Colour carries meaning only: CPU cyan, GPU magenta, running green, queued
 * yellow, finished grey, anything wrong red, directories blue. Body text
 * keeps the terminal's own colour. NO_COLOR turns every style off. */
#define BOLD "\033[1m"
#define MUTED "\033[2m"
#define CPU_COLOUR "\033[36m"
#define GPU_COLOUR "\033[35m"
#define RUN_COLOUR "\033[32m"
#define QUEUE_COLOUR "\033[33m"
#define WRONG "\033[1;31m"
#define DIR_COLOUR "\033[94m"

static const char *sgr(const char *code)
{
    return use_colour ? code : "";
}

static void restore_terminal(void)
{
    if (modes_saved) {
        tcsetattr(input_fd, TCSANOW, &saved_terminal);
        modes_saved = 0;
    }
    if (screen_active) {
        fputs("\033[0m\033[?25h\033[?1049l", stdout);
        fflush(stdout);
        screen_active = 0;
    }
    if (input_fd > STDERR_FILENO) close(input_fd);
    input_fd = -1;
}

static void stop_watch(int signal_number)
{
    (void)signal_number;
    watch_stopping = 1;
}

static void line(const char *text, const char *colour, int width, int *row, int height)
{
    char fitted[1024];
    if (*row >= height - 1) return;
    /* Count terminal columns, not bytes, and never split a character. */
    fit_columns(fitted, sizeof(fitted), text, width > 1 ? width - 1 : 0, 0);
    printf("%s%s%s\033[K\n", sgr(colour), fitted, use_colour ? "\033[0m" : "");
    ++*row;
}

static void draw_table(const struct Snapshot *snapshot, int width, int height)
{
    char text[1024], bar[49], header[256];
    int row = 0;
    /* Narrow terminals drop the GPUS column so COMMAND keeps its room, and
     * the header drops the title, then the GPU totals, until q:quit fits
     * with at least a one-character bar. junban ls still shows gpus=. */
    const int wide = width >= 50;
    const char *title = "junban  ";
    char gpus[32];
    snprintf(gpus, sizeof(gpus), "gpus %d/%d ", snapshot->busy_gpus, snapshot->total_gpus);
    for (;;) {
        snprintf(header, sizeof(header), "%scpus %d/%d [] %srunning %d queued %d q:quit",
                 title, snapshot->busy, snapshot->total, gpus, snapshot->running, snapshot->queued);
        if ((int)strlen(header) + 1 <= width - 1 || (!*title && !*gpus)) break;
        if (*title) title = "";
        else gpus[0] = 0;
    }
    int bars = width - 1 - (int)strlen(header);
    if (bars < 1) bars = 1;
    if (bars > 48) bars = 48;
    int filled = snapshot->total ? (int)((long long)bars * snapshot->busy / snapshot->total) : 0;
    for (int i = 0; i < bars; ++i) bar[i] = i < filled ? '#' : '-';
    bar[bars] = 0;
    snprintf(text, sizeof(text), "%scpus %d/%d [%s] %srunning %d queued %d q:quit",
             title, snapshot->busy, snapshot->total, bar, gpus, snapshot->running,
             snapshot->queued);
    line(text, "\033[36m", width, &row, height);
    const int states[] = {RUNNING, QUEUED, FINISHED};
    const char *names[] = {"RUNNING", "QUEUED", "DONE"};
    const char *colours[] = {"\033[32m", "\033[33m", "\033[2m"};
    int counts[3] = {0}, shown[3] = {0};
    int id_width = 2, cpu_width = 4, gpu_width = wide ? 5 : 0; /* GPUS plus its space */
    for (size_t i = 0; i < snapshot->count; ++i) {
        const struct Job_view *v = &snapshot->jobs[i];
        for (int j = 0; j < 3; ++j) if (v->state == states[j]) ++counts[j];
        int n = snprintf(text, sizeof(text), "%d", v->jobid);
        if (n > id_width) id_width = n;
        n = snprintf(text, sizeof(text), "%d", v->cpus);
        if (n > cpu_width) cpu_width = n;
    }
    /* Share spare rows across nonempty sections; finished history is capped at five. */
    int spare = height - 5;
    for (int j = 0; j < 3; ++j) if (counts[j]) --spare;
    while (spare > 0) {
        int progress = 0;
        for (int j = 0; j < 3 && spare > 0; ++j)
            if (shown[j] < counts[j] && (j != 2 || shown[j] < 5)) {
                ++shown[j]; --spare; progress = 1;
            }
        if (!progress) break;
    }
    for (int section = 0; section < 3; ++section) {
        line(names[section], "\033[1m", width, &row, height);
        if (!counts[section]) continue;
        int time_width = 7, detail_width = section == 2 ? (width < 50 ? 10 : 11) : 8;
        int dir_width = 16;
        /* Reserve COMMAND first, then shrink DIR only on narrow terminals. */
        int command_width = width - 1 - (id_width + dir_width + cpu_width + gpu_width +
                                         time_width + detail_width + 6);
        if (command_width < 7) {
            dir_width -= 7 - command_width;
            if (dir_width < 3) dir_width = 3;
        }
        snprintf(text, sizeof(text), " %*s %-*s %*s %s%-*s %-*s COMMAND",
                 id_width, "ID", dir_width, "DIR", cpu_width, "CPUS", wide ? "GPUS " : "", time_width,
                 section == 1 ? "WAITED" : "ELAPSED", detail_width, section == 2 ? "RESULT" : "EST");
        line(text, "\033[2m", width, &row, height);
        int count = 0;
        for (size_t i = 0; i < snapshot->count && count < shown[section]; ++i) {
            const struct Job_view *v = &snapshot->jobs[i];
            if (v->state != states[section]) continue;
            char timing[48], detail[64] = "", cpus[32], gpu_cell[16] = "", dir[sizeof(v->label) + 16];
            if (v->unspecified) snprintf(cpus, sizeof(cpus), "all");
            else snprintf(cpus, sizeof(cpus), "%d", v->cpus);
            format_duration(timing, sizeof(timing), (unsigned long long)
                            (section == 1 ? v->waited : v->elapsed));
            int over = section == 0 && v->estimate && v->elapsed > (double)v->estimate;
            if (section == 2) view_status(v, detail, sizeof(detail));
            else if (section == 0 && (v->killed || v->client_lost))
                snprintf(detail, sizeof(detail), "stopping");
            else if (v->estimate) {
                char estimate[48];
                format_duration(estimate, sizeof(estimate), v->estimate);
                snprintf(detail, sizeof(detail), "%s%s", estimate, over ? " OVER" : "");
            }
            fit_columns(dir, sizeof(dir), v->label, dir_width, 1);
            if (wide) snprintf(gpu_cell, sizeof(gpu_cell), "%4d ", v->gpus);
            snprintf(text, sizeof(text), " %*d %s %*s %s%-*s %-*s %s",
                     id_width, v->jobid, dir, cpu_width, cpus, gpu_cell,
                     time_width, timing, detail_width, detail, v->command);
            line(text, over ? "\033[1;31m" : colours[section], width, &row, height);
            ++count;
        }
        if (count < counts[section] && section != 2) {
            snprintf(text, sizeof(text), " ... %d more (junban ls)", counts[section] - count);
            line(text, "\033[2m", width, &row, height);
        }
    }
}

/* One screen row built from styled pieces. Columns are counted on the
 * visible text only, so a cut never splits a character or an escape. */
struct row {
    char text[4096];
    size_t used;
    int columns, limit;
};

static void row_start(struct row *r, int width)
{
    r->text[0] = 0;
    r->used = 0;
    r->columns = 0;
    r->limit = width > 1 ? width - 1 : 0;
}

static void row_raw(struct row *r, const char *s)
{
    int n = snprintf(r->text + r->used, sizeof(r->text) - r->used, "%s", s);
    if (n > 0) r->used += (size_t)n < sizeof(r->text) - r->used ? (size_t)n : sizeof(r->text) - r->used - 1;
}

static void put(struct row *r, const char *style, const char *s)
{
    char fitted[1024];
    if (r->columns >= r->limit || !*s) return;
    fit_columns(fitted, sizeof(fitted), s, r->limit - r->columns, 0);
    if (!*fitted) return;
    row_raw(r, sgr(style));
    row_raw(r, fitted);
    if (use_colour && *style) row_raw(r, "\033[0m");
    r->columns += text_columns(fitted);
}

/* Pad with spaces up to an absolute column. */
static void pad_to(struct row *r, int column)
{
    if (column > r->limit) column = r->limit;
    while (r->columns < column) {
        row_raw(r, " ");
        ++r->columns;
    }
}

static void row_print(struct row *r, int *row, int height)
{
    if (*row >= height - 1) return;
    printf("%s\033[K\n", r->text);
    ++*row;
}

static void repeat(struct row *r, const char *style, const char *glyph, int count)
{
    char run[512] = "";
    size_t used = 0, n = strlen(glyph);
    for (int i = 0; i < count && used + n < sizeof(run); ++i, used += n) memcpy(run + used, glyph, n);
    run[used] = 0;
    put(r, style, run);
}

static void gauge(struct row *r, const char *name, const char *style, int busy, int total,
                  int bars, int count_width, const char *label, int value)
{
    char text[64];
    int filled = total ? (int)((long long)bars * (busy < total ? busy : total) / total) : 0;
    pad_to(r, 1);
    put(r, style, name);
    put(r, "", " ");
    repeat(r, style, use_utf8 ? "█" : "#", filled);
    repeat(r, MUTED, use_utf8 ? "░" : "-", bars - filled);
    snprintf(text, sizeof(text), "%d/%d", busy, total);
    pad_to(r, r->columns + 2 + count_width - (int)strlen(text));
    put(r, "", text);
    pad_to(r, r->columns + 3);
    put(r, MUTED, label);
    snprintf(text, sizeof(text), " %d", value);
    put(r, "", text);
}

static void header(const struct Snapshot *snapshot, int width, int *row, int height)
{
    struct row r;
    char clock[16], cpus[32], gpus[32];
    time_t now = time(NULL);
    strftime(clock, sizeof(clock), "%H:%M:%S", localtime(&now));
    row_start(&r, width);
    put(&r, BOLD, "junban");
    /* The clock shows the screen is live; it yields to q:quit when narrow. */
    if (r.limit - r.columns >= 2 + 8 + 2 + 6) {
        pad_to(&r, r.limit - (8 + 2 + 6));
        put(&r, MUTED, clock);
        put(&r, "", "  ");
    } else pad_to(&r, r.limit - 6);
    put(&r, MUTED, "q:quit");
    row_print(&r, row, height);
    snprintf(cpus, sizeof(cpus), "%d/%d", snapshot->busy, snapshot->total);
    snprintf(gpus, sizeof(gpus), "%d/%d", snapshot->busy_gpus, snapshot->total_gpus);
    int count_width = (int)(strlen(cpus) > strlen(gpus) ? strlen(cpus) : strlen(gpus));
    char running[32], queued[32];
    int value_width = snprintf(running, sizeof(running), "%d", snapshot->running);
    int n = snprintf(queued, sizeof(queued), "%d", snapshot->queued);
    if (n > value_width) value_width = n;
    /* " CPU " + bar + "  N/M" + "   running N" */
    int bars = width - 1 - 5 - 2 - count_width - 3 - 7 - 1 - value_width;
    if (bars > 48) bars = 48;
    if (bars < 1) bars = 1;
    row_start(&r, width);
    gauge(&r, "CPU", CPU_COLOUR, snapshot->busy, snapshot->total, bars, count_width,
          "running", snapshot->running);
    row_print(&r, row, height);
    row_start(&r, width);
    gauge(&r, "GPU", GPU_COLOUR, snapshot->busy_gpus, snapshot->total_gpus, bars, count_width,
          "queued ", snapshot->queued);
    row_print(&r, row, height);
}

static int succeeded(const struct Job_view *v)
{
    return !v->killed && !v->client_lost && !v->signal && !v->status;
}

/* The directory with a trailing slash, cut in the middle of the name so the
 * slash always shows that it is a directory. */
static void dir_text(char *dst, size_t size, const char *label, int columns)
{
    if (text_columns(label) + 1 <= columns) {
        snprintf(dst, size, "%s/", label);
        return;
    }
    char cut[256];
    fit_columns(cut, sizeof(cut), label, columns > 2 ? columns - 2 : 0, 0);
    snprintf(dst, size, "%s%s/", cut, use_utf8 ? "…" : "~");
}

/* RUNNING: elapsed, with "/ EST", a progress bar and the time left (or
 * OVER) when an estimate was given. QUEUED: the time waited. DONE: the
 * result, the run time and the clock time it ended. */
static void timing(struct row *r, const struct Job_view *v, int section, int end)
{
    char elapsed[48], estimate[48], tail[64], text[64];
    format_duration(elapsed, sizeof(elapsed), (unsigned long long)
                    (section == 1 ? v->waited : v->elapsed));
    if (section == 1) {
        put(r, MUTED, "waited ");
        put(r, "", elapsed);
        return;
    }
    if (section == 2) {
        int ok = succeeded(v);
        const char *quiet = ok ? MUTED : "";
        if (ok) snprintf(text, sizeof(text), "ok");
        else if (v->killed) snprintf(text, sizeof(text), "killed");
        else if (v->client_lost) snprintf(text, sizeof(text), "client-lost");
        else if (v->signal) snprintf(text, sizeof(text), "signal %d", v->signal);
        else snprintf(text, sizeof(text), "exit %d", v->status);
        int start = r->columns;
        put(r, ok ? RUN_COLOUR : WRONG, text);
        pad_to(r, start + 9 > r->columns + 1 ? start + 9 : r->columns + 1);
        put(r, quiet, elapsed);
        if (v->ended) {
            time_t ended = (time_t)v->ended, now = time(NULL);
            struct tm then = *localtime(&ended), today = *localtime(&now);
            strftime(text, sizeof(text), then.tm_year == today.tm_year &&
                     then.tm_yday == today.tm_yday ? "  ended %H:%M" : "  ended %m-%d %H:%M",
                     &then);
            put(r, MUTED, text);
        }
        return;
    }
    if (v->killed || v->client_lost) {
        put(r, "", elapsed);
        put(r, WRONG, "  stopping");
        return;
    }
    if (!v->estimate) {
        put(r, "", elapsed);
        return;
    }
    int over = v->elapsed > (double)v->estimate;
    format_duration(estimate, sizeof(estimate), v->estimate);
    if (over) {
        format_duration(text, sizeof(text), (unsigned long long)(v->elapsed - (double)v->estimate));
        snprintf(tail, sizeof(tail), "OVER +%s", text);
    } else {
        /* Time left is a forecast, so whole minutes are precise enough. */
        unsigned long long left = (unsigned long long)((double)v->estimate - v->elapsed);
        if (left >= 60) left = (left + 59) / 60 * 60;
        format_duration(text, sizeof(text), left);
        snprintf(tail, sizeof(tail), "%s left", text);
    }
    /* Stay inside the timing field so the resources after it stay visible:
     * drop "/ EST" first, then the elapsed time, when they do not fit. */
    snprintf(text, sizeof(text), " / %s ", estimate);
    int room = end - r->columns, saved_limit = r->limit;
    if ((int)(strlen(elapsed) + strlen(text) + strlen(tail)) > room)
        snprintf(text, sizeof(text), " ");
    if ((int)(strlen(elapsed) + strlen(text) + strlen(tail)) <= room)
        put(r, over ? WRONG : "", elapsed);
    else
        text[0] = 0;
    put(r, MUTED, text);
    int bars = end - r->columns - 1 - (int)strlen(tail);
    if (bars > 20) bars = 20;
    if (bars >= 4) {
        int filled = over ? bars : (int)(bars * v->elapsed / (double)v->estimate);
        repeat(r, over ? WRONG : RUN_COLOUR, use_utf8 ? "━" : "=", filled);
        repeat(r, MUTED, use_utf8 ? "─" : "-", bars - filled);
        put(r, "", " ");
    }
    if (end < r->limit) r->limit = end;
    put(r, over ? WRONG : "", tail);
    r->limit = saved_limit;
}

/* Without UTF-8, every multibyte character in job text becomes one "?", so
 * the screen stays ASCII and each byte is one column. */
static void ascii_only(char *s)
{
    char *out = s;
    for (const unsigned char *c = (const unsigned char *)s; *c; ++c) {
        if (*c < 0x80) *out++ = (char)*c;
        else if ((*c & 0xC0) != 0x80) *out++ = '?';
    }
    *out = 0;
}

/* Each job takes two rows: its ID and command, then where it runs, its
 * timing, the resources it holds and its tag, in fixed columns. */
static void card(const struct Job_view *v, int section, int width, int id_width,
                 int tag_width, int *row, int height)
{
    struct row r;
    char text[512];
    const int resource_width = 15, indent = 1 + id_width + 2;
    int dir_width = width - 1 - indent - 4 - resource_width - tag_width - 24;
    if (dir_width > 16) dir_width = 16;
    if (dir_width < 8) dir_width = 8;
    int time_width = width - 1 - indent - dir_width - 4 - resource_width - tag_width;
    if (time_width < 24) time_width = 24;
    if (time_width > 40) time_width = 40;
    int ok = section == 2 && succeeded(v), quiet = ok;
    const char *id_style = section == 0 ? RUN_COLOUR : section == 1 ? QUEUE_COLOUR :
                           ok ? MUTED : WRONG;
    row_start(&r, width);
    if (section == 2) put(&r, ok ? RUN_COLOUR : WRONG, ok ? (use_utf8 ? "✓" : "+") :
                          (use_utf8 ? "✗" : "!"));
    pad_to(&r, 1);
    snprintf(text, sizeof(text), "%*d", id_width, v->jobid);
    put(&r, id_style, text);
    pad_to(&r, indent);
    put(&r, quiet ? MUTED : "", v->command);
    row_print(&r, row, height);
    row_start(&r, width);
    pad_to(&r, indent);
    dir_text(text, sizeof(text), v->label, dir_width);
    put(&r, quiet ? MUTED : DIR_COLOUR, text);
    pad_to(&r, indent + dir_width + 2);
    int time_end = indent + dir_width + 2 + time_width;
    timing(&r, v, section, time_end);
    if (section != 2) {
        pad_to(&r, r.columns + 2 > time_end + 2 ? r.columns + 2 : time_end + 2);
        if (v->unspecified) snprintf(text, sizeof(text), "cpus all");
        else snprintf(text, sizeof(text), "cpus %d", v->cpus);
        put(&r, CPU_COLOUR, text);
        if (v->gpus) {
            snprintf(text, sizeof(text), " gpus %d", v->gpus);
            put(&r, GPU_COLOUR, text);
        }
    }
    if (v->tag[0]) {
        pad_to(&r, r.columns + 2 > time_end + 2 + resource_width ? r.columns + 2 :
                   time_end + 2 + resource_width);
        snprintf(text, sizeof(text), "#%s", v->tag);
        put(&r, MUTED, text);
    }
    row_print(&r, row, height);
}

static void draw_cards(const struct Snapshot *snapshot, int width, int height)
{
    const int states[] = {RUNNING, QUEUED, FINISHED};
    const char *names[] = {"RUNNING", "QUEUED", "DONE"};
    const char *styles[] = {BOLD RUN_COLOUR, BOLD QUEUE_COLOUR, BOLD};
    int counts[3] = {0}, shown[3] = {0}, id_width = 2, tag_width = 0, row = 0;
    char text[64];
    for (size_t i = 0; i < snapshot->count; ++i) {
        const struct Job_view *v = &snapshot->jobs[i];
        for (int j = 0; j < 3; ++j) if (v->state == states[j]) ++counts[j];
        int n = snprintf(text, sizeof(text), "%d", v->jobid);
        if (n > id_width) id_width = n;
        /* Room for "  #tag", up to 16 columns, when any job has a tag. */
        n = v->tag[0] ? 3 + text_columns(v->tag) : 0;
        if (n > tag_width) tag_width = n > 16 ? 16 : n;
    }
    header(snapshot, width, &row, height);
    /* Three header rows, a blank row and a heading per section, and the
     * last row left empty. Jobs share the rest two rows at a time, with
     * one row kept for "... N more" in a section that has jobs. */
    int spare = height - 10;
    for (int j = 0; j < 2; ++j) if (counts[j]) --spare;
    while (spare >= 2) {
        int progress = 0;
        for (int j = 0; j < 3 && spare >= 2; ++j)
            if (shown[j] < counts[j] && (j != 2 || shown[j] < 5)) {
                ++shown[j]; spare -= 2; progress = 1;
            }
        if (!progress) break;
    }
    for (int section = 0; section < 3; ++section) {
        struct row r;
        row_start(&r, width);
        row_print(&r, &row, height);
        row_start(&r, width);
        put(&r, styles[section], names[section]);
        if (section != 2) {
            snprintf(text, sizeof(text), " %d", counts[section]);
            put(&r, MUTED, text);
        }
        row_print(&r, &row, height);
        int count = 0;
        for (size_t i = 0; i < snapshot->count && count < shown[section]; ++i) {
            const struct Job_view *v = &snapshot->jobs[i];
            if (v->state != states[section]) continue;
            card(v, section, width, id_width, tag_width, &row, height);
            ++count;
        }
        if (count < counts[section] && section != 2) {
            row_start(&r, width);
            snprintf(text, sizeof(text), " ... %d more (junban ls)", counts[section] - count);
            put(&r, MUTED, text);
            row_print(&r, &row, height);
        }
    }
}

/* Terminals of 64 columns or more get two-row cards; narrower ones keep
 * the one-row table. */
static void draw(const struct Snapshot *snapshot)
{
    struct winsize window = {0};
    int width = 80, height = 24;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &window);
    if (window.ws_col) width = window.ws_col;
    if (window.ws_row) height = window.ws_row;
    fputs("\033[H\033[2J", stdout);
    if (width >= 64) draw_cards(snapshot, width, height);
    else draw_table(snapshot, width, height);
    fflush(stdout);
}

void c_watch(void)
{
    struct termios raw;
    struct sigaction action = {0};
    struct Snapshot snapshot = {0};
    const char *no_colour = getenv("NO_COLOR"), *locale = getenv("LC_ALL");
    use_colour = !no_colour || !*no_colour;
    if (!locale || !*locale) locale = getenv("LC_CTYPE");
    if (!locale || !*locale) locale = getenv("LANG");
    use_utf8 = 0;
    for (const char *c = locale; c && *c && !use_utf8; ++c)
        use_utf8 = !strncasecmp(c, "utf-8", 5) || !strncasecmp(c, "utf8", 4);
    input_fd = isatty(STDIN_FILENO) ? STDIN_FILENO : open("/dev/tty", O_RDONLY);
    if (input_fd < 0 || tcgetattr(input_fd, &saved_terminal) < 0)
        error("cannot read terminal input");
    atexit(restore_terminal);
    modes_saved = 1;
    action.sa_handler = stop_watch;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    sigaction(SIGHUP, &action, NULL);
    raw = saved_terminal;
    raw.c_lflag &= (tcflag_t)~(ICANON | ECHO);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(input_fd, TCSANOW, &raw) < 0) error("cannot set terminal modes");
    double refresh = 0;
    while (!watch_stopping) {
        double now = monotime();
        if (now >= refresh) {
            if (!fetch_snapshot(&snapshot)) {
                free(snapshot.jobs);
                error("queue disconnected while watching");
            }
            if (watch_stopping) break;
            for (size_t i = 0; !use_utf8 && i < snapshot.count; ++i) {
                struct Job_view *v = &snapshot.jobs[i];
                ascii_only(v->label);
                ascii_only(v->tag);
                ascii_only(v->log);
                ascii_only(v->command);
            }
            /* Read the entire reply before the first stdout write too. */
            if (!screen_active) {
                screen_active = 1;
                fputs("\033[?1049h\033[?25l", stdout);
            }
            draw(&snapshot);
            refresh = monotime() + 2;
        }
        double delay = refresh - monotime();
        if (delay < 0) delay = 0;
        struct pollfd input = {input_fd, POLLIN, 0};
        int ready = poll(&input, 1, (int)(delay * 1000 + 0.999));
        if (ready < 0 && errno != EINTR) error("cannot read terminal");
        if (ready > 0) {
            char keys[64];
            ssize_t n = read(input_fd, keys, sizeof(keys));
            if (n <= 0 || memchr(keys, 'q', (size_t)n)) break;
        }
    }
    free(snapshot.jobs);
    restore_terminal();
}
