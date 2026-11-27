/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2013  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <math.h>
#include "main.h"

/* Slot accounting. junban treats the GPU as one device a job uses alone;
 * it does not assign devices, so it never counts more than one. */
int busy_slots = 0;
int max_slots = 1;
int busy_gpus = 0;
int max_gpus = 1;

/* Globals */
static struct Job *firstjob = 0;
static struct Job *first_finished_job = 0;
static int jobids = 0;
int max_jobs;

static void shortage(const struct Job *job, int *short_cpus, int *short_gpus);


static void send_list_line(int s, const char * str)
{
    struct msg m;

    /* Message */
    m.type = LIST_LINE;
    m.u.size = strlen(str) + 1;

    send_msg(s, &m);

    /* Send the line */
    send_bytes(s, str, m.u.size);
}

static struct Job * findjob(int jobid)
{
    struct Job *p;

    /* Show Queued or Running jobs */
    p = firstjob;
    while(p != 0)
    {
        if (p->jobid == jobid)
            return p;
        p = p->next;
    }

    return 0;
}

static int count_not_finished_jobs()
{
    int count=0;
    struct Job *p;

    /* Show Queued or Running jobs */
    p = firstjob;
    while(p != 0)
    {
        ++count;
        p = p->next;
    }
    return count;
}

void s_mark_job_running(int jobid)
{
    struct Job *p;
    p = findjob(jobid);
    if (!p)
        error("Cannot mark the jobid %i RUNNING.", jobid);
    p->state = RUNNING;
    p->started_at = monotime();
}

const char * jstate2string(enum Jobstate s)
{
    const char * jobstate;
    switch(s)
    {
        case QUEUED:
            jobstate = "queued";
            break;
        case RUNNING:
            jobstate = "running";
            break;
        case FINISHED:
            jobstate = "finished";
            break;
        case HOLDING_CLIENT:
            jobstate = "queue-full";
            break;
        default:
            jobstate = "unknown";
            break;
    }
    return jobstate;
}

int s_has_jobs(void)
{
    return firstjob != NULL;
}

static void send_view(int socket, const struct Job *job)
{
    struct msg message = {0};
    struct Job_view view;
    job_view(job, &view);
    message.type = SNAPSHOT_ROW;
    message.u.max_slots = max_slots;
    send_msg(socket, &message);
    send_bytes(socket, (const char *)&view, sizeof(view));
}

void s_snapshot(int socket, int all)
{
    struct Job *p, *finished[20];
    struct msg end = {0};
    int count = 0, limit = all ? 20 : 5;
    for (p = firstjob; p; p = p->next)
        if (p->state == RUNNING) send_view(socket, p);
    for (p = firstjob; p; p = p->next)
        if (p->state == QUEUED) send_view(socket, p);
    /* Listings show only the newest 20 of the kept results. */
    for (p = first_finished_job; p; p = p->next)
        finished[count++ % 20] = p;
    for (int i = count - 1; i >= 0 && i >= count - 20 && limit; --i, --limit)
        send_view(socket, finished[i % 20]);
    end.type = SNAPSHOT_END;
    end.u.snapshot_end.max_slots = max_slots;
    end.u.snapshot_end.next_jobid = jobids;
    end.u.snapshot_end.max_gpus = max_gpus;
    send_msg(socket, &end);
}

void s_wait_notice(int socket, int jobid)
{
    struct Job *job = findjob(jobid), *p;
    struct msg end = {0};
    char line[512], timing[128], cpus[32];
    struct Job_view queued;
    int ahead = 0, short_cpus = 0, short_gpus = 0;
    if (!job || job->state != QUEUED || job->kill_requested) return;
    for (p = firstjob; p && p != job; p = p->next) ++ahead;
    shortage(job, &short_cpus, &short_gpus);
    job_view(job, &queued);
    view_cpus(&queued, cpus, sizeof(cpus));
    snprintf(line, sizeof(line), "id=%d queued, cpus=%s, gpus=%d, %d ahead, waiting for %s\n",
             jobid, cpus, queued.gpus, ahead,
             short_cpus && short_gpus ? "cpus and gpus" : short_gpus ? "gpus" : "cpus");
    send_list_line(socket, line);
    for (p = firstjob; p; p = p->next) {
        struct Job_view view;
        if (p->state != RUNNING) continue;
        job_view(p, &view);
        view_fields(&view, timing, sizeof(timing));
        view_cpus(&view, cpus, sizeof(cpus));
        snprintf(line, sizeof(line), "  running id=%d dir=%s cpus=%s gpus=%d %s\n",
                 p->jobid, view.label, cpus, view.gpus, timing);
        send_list_line(socket, line);
    }
    end.type = NOTICE_END;
    send_msg(socket, &end);
}

static struct Job * newjobptr()
{
    struct Job *p;

    if (firstjob == 0)
    {
        firstjob = (struct Job *) malloc(sizeof(*firstjob));
        firstjob->next = 0;
        firstjob->command = 0;
        return firstjob;
    }

    p = firstjob;
    while(p->next != 0)
        p = p->next;

    p->next = (struct Job *) malloc(sizeof(*p));
    p->next->next = 0;
    p->next->command = 0;

    return p->next;
}

/* Returns job id or -1 on error */
/* Read a NUL-terminated string of size bytes into a new buffer, or leave
 * *dst NULL for size 0. Returns 0 if the client went away. */
static int recv_string(int s, int size, char **dst)
{
    if (size <= 0) return 1;
    if (!(*dst = malloc((size_t)size)))
        error("Cannot allocate memory in s_newjob (%i)", size);
    if (recv_bytes(s, *dst, size) != size) return 0;
    (*dst)[size - 1] = '\0';
    return 1;
}

int s_newjob(int s, struct msg *m)
{
    struct Job *p;
    char *log_dir = NULL;

    p = newjobptr();

    p->jobid = jobids++;
    if (count_not_finished_jobs() <= max_jobs)
        p->state = QUEUED;
    else
        p->state = HOLDING_CLIENT;
    p->cpus_unspecified = m->u.newjob.num_slots == 0;
    p->estimate = 0;
    p->queued_at = monotime();
    p->started_at = p->finished_at = 0;
    p->finished_wall = 0;
    memset(&p->result, 0, sizeof(p->result));
    p->client_lost = 0;
    p->finish_pending = p->stop_kill_sent = 0;
    p->stop_deadline = 0;
    p->delayed = 0;
    p->num_slots = p->cpus_unspecified ? max_slots : m->u.newjob.num_slots;
    p->num_gpus = m->u.newjob.num_gpus;
    p->pid = 0;
    p->kill_requested = 0;
    p->should_keep_finished = p->state != HOLDING_CLIENT;
    p->label = 0;
    p->tag = 0;
    p->log = 0;

    if (recv_bytes(s, (char *)&p->estimate, sizeof(p->estimate)) != (int)sizeof(p->estimate))
        goto disconnected;

    if (!recv_string(s, m->u.newjob.command_size, &p->command) ||
        !recv_string(s, m->u.newjob.label_size, &p->label) ||
        !recv_string(s, m->u.newjob.tag_size, &p->tag) ||
        !recv_string(s, m->u.newjob.log_size, &log_dir))
        goto disconnected;

    /* The client sends the log directory; the file is named after the job ID. */
    if (log_dir) {
        size_t size = strlen(log_dir) + 16;
        if (!(p->log = malloc(size))) error("Cannot allocate memory in s_newjob");
        snprintf(p->log, size, "%s%d.log", log_dir, p->jobid);
        free(log_dir);
    }

    return p->jobid;
disconnected:
    free(log_dir);
    {
        struct Result result = {0};
        p->should_keep_finished = 0;
        result.errorlevel = 125;
        job_finished(&result, p->jobid);
    }
    return -1;
}

/* Scheduling.
 *
 * Jobs start in the order they were submitted. A job starts ahead of others
 * only if that costs each waiting job no more than its slack: a fifth of the
 * time it was said to take (--estimate). A job started without -c, often a
 * benchmark, and a job of unknown length have no slack.
 *
 * junban counts only on what it was told. A job without an estimate may run
 * for good, whether it is running, waiting or about to start. So the plan
 * frees what a running job holds only at a known end, and a waiting job that
 * needs more than that has no planned start yet: until it has one, jobs of
 * known length may start ahead of it, since they end. A job of unknown
 * length starts ahead of a waiting job only if all jobs of unknown length
 * together leave that job what it asks for.
 *
 * The plan places the waiting jobs in queue order, each at the earliest time
 * it fits without moving an earlier one. What starting a job ahead costs a
 * waiting job is how much later the plan then starts it. A job that ends
 * early or late moves the plan too, but that uses up no one's slack. */

#define SLACK_SHARE 0.2

/* When next_run_job last looked at the jobs. */
static double looked_at;

/* What is free from at until the next step, and after the last one for good. */
struct Step {
    double at;
    int cpus, gpus;
};

struct Plan {
    struct Step *step;
    int n, cap;
    int held_cpus, held_gpus; /* By the jobs of unknown length that run. */
};

static double slack(const struct Job *job)
{
    return job->cpus_unspecified ? 0 : SLACK_SHARE * (double)job->estimate;
}

/* How long a job holds what it uses, as far as junban was told. */
static double length(const struct Job *job)
{
    return job->estimate ? (double)job->estimate : INFINITY;
}

/* When a running job is expected to end. Most overruns are short, so a job
 * past its estimate is expected a minute later, then 15, 30, 60 minutes
 * later and so on (Tsafrir, Etsion and Feitelson, 2007). */
static double expected_end(const struct Job *job, double now)
{
    double end = job->started_at + length(job), more = 60;
    for (; end <= now; more = more == 60 ? 900 : more * 2) end += more;
    return end;
}

static void plan_insert(struct Plan *pl, int i, struct Step step)
{
    if (pl->n == pl->cap) {
        pl->cap = pl->cap ? pl->cap * 2 : 64;
        if (!(pl->step = realloc(pl->step, (size_t)pl->cap * sizeof(*pl->step))))
            error("Cannot allocate memory for the plan");
    }
    memmove(pl->step + i + 1, pl->step + i, (size_t)(pl->n - i) * sizeof(*pl->step));
    pl->step[i] = step;
    ++pl->n;
}

/* Start a step at t, if none does, and return its index. */
static int plan_split(struct Plan *pl, double t)
{
    int i = pl->n - 1;
    while (pl->step[i].at > t) --i;
    if (pl->step[i].at == t) return i;
    plan_insert(pl, i + 1, (struct Step){t, pl->step[i].cpus, pl->step[i].gpus});
    return i + 1;
}

/* Take cpus and gpus from start for run seconds. */
static void plan_take(struct Plan *pl, double start, double run, int cpus, int gpus)
{
    int i = plan_split(pl, start);
    int end = start + run < INFINITY ? plan_split(pl, start + run) : pl->n;
    for (; i < end; ++i) {
        pl->step[i].cpus -= cpus;
        pl->step[i].gpus -= gpus;
    }
}

/* A job that runs gives back what it holds at its end, if that is known. */
static void plan_run(struct Plan *pl, const struct Job *job, double end)
{
    if (end < INFINITY) plan_take(pl, end, INFINITY, -job->num_slots, -job->num_gpus);
    else {
        pl->held_cpus += job->num_slots;
        pl->held_gpus += job->num_gpus;
    }
}

/* What is free from now on, as the running jobs end, with the waiting job
 * starting, if any, started now. */
static void plan_running(struct Plan *pl, double now, const struct Job *starting)
{
    const struct Job *p;
    pl->n = pl->held_cpus = pl->held_gpus = 0;
    plan_insert(pl, 0, (struct Step){now, max_slots - busy_slots, max_gpus - busy_gpus});
    for (p = firstjob; p; p = p->next)
        if (p->state == RUNNING) plan_run(pl, p, expected_end(p, now));
    if (starting) {
        plan_take(pl, now, INFINITY, starting->num_slots, starting->num_gpus);
        plan_run(pl, starting, now + length(starting));
    }
}

/* The earliest time job fits for its whole length; INFINITY if never. */
static double plan_fit(const struct Plan *pl, const struct Job *job)
{
    double run = length(job);
    int i, j;
    for (i = 0; i < pl->n; i = j + 1) {
        for (j = i; j < pl->n && pl->step[j].at < pl->step[i].at + run; ++j)
            if (pl->step[j].cpus < job->num_slots || pl->step[j].gpus < job->num_gpus) break;
        if (j == pl->n || pl->step[j].at >= pl->step[i].at + run) return pl->step[i].at;
    }
    return INFINITY;
}

/* Place the waiting jobs in queue order. With no job started ahead, note
 * each planned start. With one, note how much later each job then starts,
 * and return 0 once that is more than the job's slack has left.
 *
 * A job of unknown length may hold what it takes for good. So it is placed
 * only if the jobs of unknown length that run, and it, still leave each job
 * ahead of it that has no place what that job asks for, of whatever it uses
 * itself. A job ahead that has a place is covered by its planned start. */
static int plan_waiting(struct Plan *pl, const struct Job *ahead)
{
    int most_cpus = 0, most_gpus = 0; /* The most a job with no place asks for. */
    struct Job *p;
    for (p = firstjob; p; p = p->next) {
        double start = INFINITY;
        if (p->state != QUEUED || p == ahead) continue;
        if (p->estimate ||
            (pl->held_cpus + p->num_slots + most_cpus <= max_slots &&
             (!p->num_gpus || pl->held_gpus + p->num_gpus + most_gpus <= max_gpus)))
            start = plan_fit(pl, p);
        if (start == INFINITY) {
            if (p->num_slots > most_cpus) most_cpus = p->num_slots;
            if (p->num_gpus > most_gpus) most_gpus = p->num_gpus;
        }
        if (!ahead) p->planned = start;
        else {
            /* A job with no planned start has none to lose. */
            p->late = start > p->planned ? start - p->planned : 0;
            if (p->delayed + p->late > slack(p)) return 0;
        }
        if (start < INFINITY) plan_take(pl, start, length(p), p->num_slots, p->num_gpus);
    }
    return 1;
}

/* How much of the machine a job fills. The GPU counts as much as all CPUs:
 * it is the scarcer, and every job that uses it uses a CPU too. */
static double fill(const struct Job *job)
{
    return (double)job->num_slots / max_slots +
           (max_gpus ? (double)job->num_gpus / max_gpus : 0);
}

static int fuller_first(const void *a, const void *b)
{
    const struct Job *x = *(const struct Job *const *)a, *y = *(const struct Job *const *)b;
    if (fill(x) != fill(y)) return fill(x) < fill(y) ? 1 : -1;
    return (x->jobid > y->jobid) - (x->jobid < y->jobid);
}

/* The job to start now, or -1. Of the jobs that may start, the one that
 * fills the most of the machine goes first, so the GPU does not sit idle
 * behind CPU jobs that can wait. */
int next_run_job(void)
{
    int free_cpus = max_slots - busy_slots, free_gpus = max_gpus - busy_gpus;
    double now = looked_at = monotime();
    struct Plan pl = {0};
    struct Job *p, **fits, *chosen = NULL;
    int n = 0, i;
    for (p = firstjob; p; p = p->next) n += p->state == QUEUED;
    if (!(fits = malloc((size_t)(n + 1) * sizeof(*fits))))
        error("Cannot allocate memory in next_run_job");
    n = 0;
    for (p = firstjob; p; p = p->next)
        if (p->state == QUEUED && !p->kill_requested && p->num_slots <= free_cpus &&
            p->num_gpus <= free_gpus)
            fits[n++] = p;
    if (n) {
        qsort(fits, (size_t)n, sizeof(*fits), fuller_first);
        plan_running(&pl, now, NULL);
        plan_waiting(&pl, NULL);
    }
    /* A job the plan cannot place, as one of unknown length that would not
     * leave a job ahead of it enough, does not start. */
    for (i = 0; i < n && !chosen; ++i) {
        if (fits[i]->planned == INFINITY) continue;
        plan_running(&pl, now, fits[i]);
        if (plan_waiting(&pl, fits[i])) chosen = fits[i];
    }
    free(fits);
    free(pl.step);
    if (!chosen) return -1;
    for (p = firstjob; p; p = p->next)
        if (p->state == QUEUED && p != chosen) p->delayed += p->late;
    busy_slots += chosen->num_slots;
    busy_gpus += chosen->num_gpus;
    return chosen->jobid;
}

/* What a waiting job lacks once the jobs ahead of it take what they ask
 * for, for the wait notice. */
static void shortage(const struct Job *job, int *short_cpus, int *short_gpus)
{
    int free_cpus = max_slots - busy_slots, free_gpus = max_gpus - busy_gpus;
    const struct Job *p;
    for (p = firstjob; p != job; p = p->next) {
        if (p->state != QUEUED) continue;
        free_cpus = free_cpus > p->num_slots ? free_cpus - p->num_slots : 0;
        free_gpus = free_gpus > p->num_gpus ? free_gpus - p->num_gpus : 0;
    }
    *short_cpus = job->num_slots > free_cpus;
    *short_gpus = job->num_gpus > free_gpus;
}

/* junban keeps the results of the last 1000 finished jobs, so junban wait
 * can report a job that finished long ago. ls shows the newest 20. */
static int get_max_finished_jobs()
{
    return 1000;
}

/* A negative ID asks for the ID the next job will get. */
void s_job_info(int socket, int jobid)
{
    struct Job *p;
    if (jobid < 0) {
        struct msg next = {0};
        next.type = JOB_INFO;
        next.u.jobid = jobids;
        send_msg(socket, &next);
        return;
    }
    p = findjob(jobid);
    if (!p)
        for (p = first_finished_job; p && p->jobid != jobid; p = p->next) {}
    if (p) send_view(socket, p);
    else {
        struct msg unknown = {0};
        unknown.type = NEWJOB_NOK;
        send_msg(socket, &unknown);
    }
}

/* Add the job to the finished queue. */
static void new_finished_job(struct Job *j)
{
    struct Job *p;
    int count, max;

    max = get_max_finished_jobs();
    count = 0;

    if (first_finished_job == 0 && count < max)
    {
        first_finished_job = j;
        first_finished_job->next = 0;
        return;
    }

    ++count;

    /* Case of max = 0 */
    if (count > max)
    {
        return;
    }

    p = first_finished_job;
    while(p->next != 0)
    {
        p = p->next;
        ++count;
    }

    /* If too many jobs, wipe out the first */
    if (count >= max)
    {
        struct Job *tmp;
        tmp = first_finished_job;
        first_finished_job = first_finished_job->next;
        free(tmp->command);
        free(tmp->label);
        free(tmp->tag);
        free(tmp->log);
        free(tmp);
    }
    p->next = j;
    p->next->next = 0;

    return;
}

static int job_is_in_state(int jobid, enum Jobstate state)
{
    struct Job *p;

    p = findjob(jobid);
    if (p == 0)
        return 0;
    if (p->state == state)
        return 1;
    return 0;
}

int job_is_holding_client(int jobid)
{
    return job_is_in_state(jobid, HOLDING_CLIENT);
}

void job_finished(const struct Result *result, int jobid)
{
    struct Job *p;

    p = findjob(jobid);
    if (p == 0)
        error("on jobid %i finished, it doesn't exist", jobid);

    /* The job may be not only in running state, but also in other states, as
     * we call this to clean up the jobs list in case of the client closing the
     * connection. */
    if (p->state == RUNNING) {
        busy_slots = busy_slots - p->num_slots;
        busy_gpus = busy_gpus - p->num_gpus;
    }

    p->state = FINISHED;
    p->result = *result;
    p->finished_at = monotime();
    p->finished_wall = (long long)time(NULL);

    /* Find the pointing node, to
     * update it removing the finished job. */
    {
        struct Job **jpointer = 0;
        struct Job *newfirst = p->next;
        if (firstjob == p)
            jpointer = &firstjob;
        else
        {
            struct Job *p2;
            p2 = firstjob;
            while(p2 != 0)
            {
                if (p2->next == p)
                {
                    jpointer = &(p2->next);
                    break;
                }
                p2 = p2->next;
            }
        }

        /* Retain accepted jobs in the finished history. */
        if (p->should_keep_finished)
            new_finished_job(p);

        /* Remove it from the run queue */
        if (jpointer == 0)
            error("Cannot remove a finished job from the "
                "queue list (jobid=%i)", p->jobid);

        *jpointer = newfirst;
        if (!p->should_keep_finished) {
            free(p->command);
            free(p->label);
            free(p->tag);
            free(p->log);
            free(p);
        }
    }
}

static void request_group_stop(struct Job *job)
{
    if (job->stop_deadline) return;
    job->stop_deadline = monotime() + 5;
    if (job->pid > 0) kill(-job->pid, SIGTERM);
}

/* Also check at the server boundary: a completion report alone cannot
 * release slots while members of the registered group still exist. */
void s_end_job(const struct Result *result, int jobid)
{
    struct Job *job = findjob(jobid);
    if (!job) return;
    if (job->state == RUNNING && process_group_alive(job->pid)) {
        job->result = *result;
        job->finish_pending = 1;
        return;
    }
    job_finished(result, jobid);
}

/* Seconds until the server should look at the jobs again unasked: shortly
 * while a stopped job's processes are going away, else when a running job
 * reaches the end expected of it at the last look, which moves the plan.
 * -1 if never. */
double s_next_wake(void)
{
    struct Job *job;
    double now = monotime(), wake = -1;
    for (job = firstjob; job; job = job->next) {
        double left;
        if (job->finish_pending || job->stop_deadline) return 0.1;
        if (job->state != RUNNING || !job->estimate) continue;
        left = expected_end(job, looked_at) - now;
        if (wake < 0 || left < wake) wake = left > 0 ? left : 0;
    }
    /* select() refuses a very long wait, and waking early costs nothing. */
    return wake > 3600 ? 3600 : wake;
}

void s_poll_jobs(void)
{
    struct Job *job = firstjob;
    double now = monotime();
    while (job) {
        struct Job *next = job->next;
        int alive = job->state == RUNNING && process_group_alive(job->pid);
        if (alive && job->stop_deadline && now >= job->stop_deadline &&
            !job->stop_kill_sent) {
            kill(-job->pid, SIGKILL);
            job->stop_kill_sent = 1;
        }
        if (job->finish_pending && !alive) {
            struct Result result = job->result;
            int id = job->jobid;
            job_finished(&result, id);
        }
        job = next;
    }
}

void s_process_runjob_ok(int jobid, int pid)
{
    struct Job *p;
    p = findjob(jobid);
    if (p == 0)
        error("Job %i already run not found on runjob_ok", jobid);
    if (p->state != RUNNING)
        error("Job %i not running, but %i on runjob_ok", jobid,
                p->state);

    p->pid = pid;
    if (p->kill_requested && pid > 0) kill(-pid, SIGTERM);
}

/* Return 2 for a queued job (caller removes its connection), 1 for running. */
int s_kill_job(int jobid)
{
    struct Job *p = findjob(jobid);
    if (!p) return 0;
    p->kill_requested = 1;
    if (p->state != RUNNING) return 2;
    request_group_stop(p);
    return 1;
}

void s_terminate_job(int jobid)
{
    struct Job *p = findjob(jobid);
    struct Result result = {0};
    if (!p) return;
    if (!p->kill_requested) p->client_lost = 1;
    if (p->state == RUNNING) request_group_stop(p);
    result.errorlevel = 125;
    s_end_job(&result, jobid);
}

void s_send_runjob(int s, int jobid)
{
    struct msg m = {0};
    struct Job *p;

    p = findjob(jobid);
    if (p == 0) 
        error("Job %i was expected to run", jobid);


    m.type = RUNJOB;

    send_msg(s, &m);
}

/* jobid is input/output. If the input is -1, it's changed to the jobid
 * removed */
/* Don't complain, if the socket doesn't exist */
/* This is called when a job finishes */
void s_set_max_slots(int new_max_slots)
{
    if (new_max_slots > 0)
        max_slots = new_max_slots;
    else
        warning("Received new_max_slots=%i", new_max_slots);
}

void s_get_max_slots(int s)
{
    struct msg m;

    /* Message */
    m.type = GET_MAX_SLOTS_OK;
    m.u.snapshot_end.max_slots = max_slots;
    m.u.snapshot_end.max_gpus = max_gpus;

    send_msg(s, &m);
}

static void dump_job_struct(FILE *out, const struct Job *p)
{
    fprintf(out, "  new_job\n");
    fprintf(out, "    jobid %i\n", p->jobid);
    fprintf(out, "    command \"%s\"\n", p->command);
    fprintf(out, "    state %s\n",
            jstate2string(p->state));
    fprintf(out, "    result.errorlevel %i\n", p->result.errorlevel);
    fprintf(out, "    pid %i\n", p->pid);
    fprintf(out, "    should_keep_finished %i\n", p->should_keep_finished);
}

void dump_jobs_struct(FILE *out)
{
    const struct Job *p;

    fprintf(out, "New_jobs\n");

    p = firstjob;
    while (p != 0)
    {
        dump_job_struct(out, p);
        p = p->next;
    }

    p = first_finished_job;
    while (p != 0)
    {
        dump_job_struct(out, p);
        p = p->next;
    }
}


/* Live upgrade: the queue state, written by the old server and read back by
 * the new one after exec. Every field is written by name, not as a raw
 * struct, so a later version can still read it; LIVE_STATE_VERSION changes
 * whenever the layout does. */
/* A failed write shows in ferror(f), which the writer checks once at the
 * end, so the old server keeps running if it cannot save. */
void live_put(FILE *f, const void *data, size_t size)
{
    if (size) fwrite(data, size, 1, f);
}

void live_get(FILE *f, void *data, size_t size)
{
    if (size && fread(data, size, 1, f) != 1) error("cannot read live state");
}

void live_put_string(FILE *f, const char *s)
{
    int size = s ? (int)strlen(s) + 1 : 0;
    live_put(f, &size, sizeof(size));
    live_put(f, s, (size_t)size);
}

char *live_get_string(FILE *f)
{
    int size;
    char *s;
    live_get(f, &size, sizeof(size));
    if (size <= 0) return NULL;
    if (!(s = malloc((size_t)size))) error("cannot allocate live state");
    live_get(f, s, (size_t)size);
    s[size - 1] = '\0';
    return s;
}

static void put_job(FILE *f, const struct Job *p)
{
    int state = p->state;
    live_put(f, &p->jobid, sizeof(p->jobid));
    live_put_string(f, p->command);
    live_put(f, &state, sizeof(state));
    live_put(f, &p->result, sizeof(p->result));
    live_put(f, &p->should_keep_finished, sizeof(p->should_keep_finished));
    live_put(f, &p->pid, sizeof(p->pid));
    live_put_string(f, p->label);
    live_put_string(f, p->tag);
    live_put_string(f, p->log);
    live_put(f, &p->num_slots, sizeof(p->num_slots));
    live_put(f, &p->num_gpus, sizeof(p->num_gpus));
    live_put(f, &p->cpus_unspecified, sizeof(p->cpus_unspecified));
    live_put(f, &p->kill_requested, sizeof(p->kill_requested));
    live_put(f, &p->client_lost, sizeof(p->client_lost));
    live_put(f, &p->estimate, sizeof(p->estimate));
    live_put(f, &p->queued_at, sizeof(p->queued_at));
    live_put(f, &p->started_at, sizeof(p->started_at));
    live_put(f, &p->finished_at, sizeof(p->finished_at));
    live_put(f, &p->finished_wall, sizeof(p->finished_wall));
    live_put(f, &p->finish_pending, sizeof(p->finish_pending));
    live_put(f, &p->stop_kill_sent, sizeof(p->stop_kill_sent));
    live_put(f, &p->stop_deadline, sizeof(p->stop_deadline));
}

static struct Job *get_job(FILE *f)
{
    struct Job *p = calloc(1, sizeof(*p));
    int state;
    if (!p) error("cannot allocate live state");
    live_get(f, &p->jobid, sizeof(p->jobid));
    p->command = live_get_string(f);
    live_get(f, &state, sizeof(state));
    p->state = (enum Jobstate)state;
    live_get(f, &p->result, sizeof(p->result));
    live_get(f, &p->should_keep_finished, sizeof(p->should_keep_finished));
    live_get(f, &p->pid, sizeof(p->pid));
    p->label = live_get_string(f);
    p->tag = live_get_string(f);
    p->log = live_get_string(f);
    live_get(f, &p->num_slots, sizeof(p->num_slots));
    live_get(f, &p->num_gpus, sizeof(p->num_gpus));
    live_get(f, &p->cpus_unspecified, sizeof(p->cpus_unspecified));
    live_get(f, &p->kill_requested, sizeof(p->kill_requested));
    live_get(f, &p->client_lost, sizeof(p->client_lost));
    live_get(f, &p->estimate, sizeof(p->estimate));
    live_get(f, &p->queued_at, sizeof(p->queued_at));
    live_get(f, &p->started_at, sizeof(p->started_at));
    live_get(f, &p->finished_at, sizeof(p->finished_at));
    live_get(f, &p->finished_wall, sizeof(p->finished_wall));
    live_get(f, &p->finish_pending, sizeof(p->finish_pending));
    live_get(f, &p->stop_kill_sent, sizeof(p->stop_kill_sent));
    live_get(f, &p->stop_deadline, sizeof(p->stop_deadline));
    return p;
}

static void put_list(FILE *f, const struct Job *p)
{
    int count = 0;
    const struct Job *q;
    for (q = p; q; q = q->next) ++count;
    live_put(f, &count, sizeof(count));
    for (; p; p = p->next) put_job(f, p);
}

static struct Job *get_list(FILE *f)
{
    struct Job *first = NULL, **next = &first;
    int count;
    live_get(f, &count, sizeof(count));
    while (count-- > 0) {
        *next = get_job(f);
        next = &(*next)->next;
    }
    return first;
}

void s_save_jobs(FILE *f)
{
    live_put(f, &jobids, sizeof(jobids));
    live_put(f, &busy_slots, sizeof(busy_slots));
    live_put(f, &busy_gpus, sizeof(busy_gpus));
    live_put(f, &max_slots, sizeof(max_slots));
    live_put(f, &max_gpus, sizeof(max_gpus));
    put_list(f, firstjob);
    put_list(f, first_finished_job);
}

void s_load_jobs(FILE *f)
{
    live_get(f, &jobids, sizeof(jobids));
    live_get(f, &busy_slots, sizeof(busy_slots));
    live_get(f, &busy_gpus, sizeof(busy_gpus));
    live_get(f, &max_slots, sizeof(max_slots));
    live_get(f, &max_gpus, sizeof(max_gpus));
    firstjob = get_list(f);
    first_finished_job = get_list(f);
}
