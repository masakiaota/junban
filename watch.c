/* Modified for junban in 2026 by junban contributors. */
/* Plain ANSI queue monitor; GPL-2.0, see COPYING. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <sys/time.h>
#include "main.h"

static struct termios saved_terminal;
static int input_fd = -1, screen_active, modes_saved;
static volatile sig_atomic_t watch_stopping;

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
    if (*row >= height - 1) return;
    int length = (int)strlen(text);
    if (length >= width) length = width - 1;
    if (length < 0) length = 0;
    /* Avoid cutting a UTF-8 command in the middle of a character. */
    while (length > 0 && ((unsigned char)text[length] & 0xc0) == 0x80) --length;
    printf("%s%.*s\033[0m\033[K\n", colour, length, text);
    ++*row;
}

static void draw(const struct Snapshot *snapshot)
{
    struct winsize window = {0};
    char text[1024], bar[49], header[256];
    int width = 80, height = 24, row = 0;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &window);
    if (window.ws_col) width = window.ws_col;
    if (window.ws_row) height = window.ws_row;
    const char *title = width >= 50 ? "junban  " : "";
    snprintf(header, sizeof(header), "%scpus %d/%d [] running %d queued %d q:quit",
             title, snapshot->busy, snapshot->total, snapshot->running, snapshot->queued);
    int bars = width - 1 - (int)strlen(header);
    if (bars < 1) bars = 1;
    if (bars > 48) bars = 48;
    int filled = snapshot->total ? (int)((long long)bars * snapshot->busy / snapshot->total) : 0;
    for (int i = 0; i < bars; ++i) bar[i] = i < filled ? '#' : '-';
    bar[bars] = 0;
    fputs("\033[H\033[2J", stdout);
    snprintf(text, sizeof(text), "%scpus %d/%d [%s] running %d queued %d q:quit",
             title, snapshot->busy, snapshot->total, bar, snapshot->running, snapshot->queued);
    line(text, "\033[36m", width, &row, height);
    const int states[] = {RUNNING, QUEUED, FINISHED};
    const char *names[] = {"RUNNING", "QUEUED", "DONE"};
    const char *colours[] = {"\033[32m", "\033[33m", "\033[2m"};
    int counts[3] = {0}, shown[3] = {0};
    int id_width = 2, cpu_width = 4;
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
        int command_width = width - 1 - (id_width + dir_width + cpu_width + time_width + detail_width + 6);
        if (command_width < 7) {
            dir_width -= 7 - command_width;
            if (dir_width < 3) dir_width = 3;
        }
        snprintf(text, sizeof(text), " %*s %-*s %*s %-*s %-*s COMMAND",
                 id_width, "ID", dir_width, "DIR", cpu_width, "CPUS", time_width,
                 section == 1 ? "WAITED" : "ELAPSED", detail_width, section == 2 ? "RESULT" : "EST");
        line(text, "\033[2m", width, &row, height);
        int count = 0;
        for (size_t i = 0; i < snapshot->count && count < shown[section]; ++i) {
            const struct Job_view *v = &snapshot->jobs[i];
            if (v->state != states[section]) continue;
            char timing[48], detail[64] = "", cpus[32];
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
            snprintf(text, sizeof(text), " %*d %-*.*s %*s %-*s %-*s %s",
                     id_width, v->jobid, dir_width, dir_width, v->label, cpu_width, cpus,
                     time_width, timing, detail_width, detail, v->command);
            line(text, over ? "\033[1;31m" : colours[section], width, &row, height);
            ++count;
        }
        if (count < counts[section] && section != 2) {
            snprintf(text, sizeof(text), " ... %d more (junban ls)", counts[section] - count);
            line(text, "\033[2m", width, &row, height);
        }
    }
    fflush(stdout);
}

void c_watch(void)
{
    struct termios raw;
    struct sigaction action = {0};
    struct Snapshot snapshot = {0};
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
