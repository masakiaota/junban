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

static int schedule(const struct Job *stop, int *short_cpus, int *short_gpus);


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
    schedule(job, &short_cpus, &short_gpus);
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
    memset(&p->result, 0, sizeof(p->result));
    p->client_lost = 0;
    p->finish_pending = p->stop_kill_sent = 0;
    p->stop_deadline = 0;
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

/* Per-resource FIFO. Jobs are taken in queue order. A job that cannot
 * start reserves what it asks for, so a later job may start only within
 * what is left after every earlier queued job's reservation. A later job
 * therefore never takes a CPU or GPU an earlier job is waiting for, yet it
 * may pass a job that waits only for a resource it does not use. Every job
 * uses at least one CPU, so once the reservations cover all CPUs nothing
 * behind can start, which keeps a job that needs all CPUs from starving.
 * Returns the job's remaining shortage in *short_cpus and *short_gpus when
 * stop is given and reached, for the wait notice. -1 if no one should run. */
static int schedule(const struct Job *stop, int *short_cpus, int *short_gpus)
{
    int free_cpus = max_slots - busy_slots, free_gpus = max_gpus - busy_gpus;
    struct Job *p;
    /* The notice scans up to its job even when no CPU is left, so it also
     * sees the GPUs that earlier jobs reserve. */
    for (p = firstjob; p != 0 && (stop || free_cpus > 0); p = p->next) {
        if (p->state != QUEUED) continue;
        /* A job being killed in the queue keeps its reservation until it goes. */
        int fits = !p->kill_requested && p->num_slots <= free_cpus &&
                   p->num_gpus <= free_gpus;
        if (p == stop) {
            *short_cpus = p->num_slots > free_cpus;
            *short_gpus = p->num_gpus > free_gpus;
            return -1;
        }
        if (fits && !stop) {
            busy_slots = busy_slots + p->num_slots;
            busy_gpus = busy_gpus + p->num_gpus;
            return p->jobid;
        }
        free_cpus = free_cpus > p->num_slots ? free_cpus - p->num_slots : 0;
        free_gpus = free_gpus > p->num_gpus ? free_gpus - p->num_gpus : 0;
    }
    return -1;
}

int next_run_job()
{
    return schedule(NULL, NULL, NULL);
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

int s_needs_poll(void)
{
    struct Job *job;
    for (job = firstjob; job; job = job->next)
        if (job->finish_pending || job->stop_deadline) return 1;
    return 0;
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

