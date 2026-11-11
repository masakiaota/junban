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

/* Slot accounting */
int busy_slots = 0;
int max_slots = 1;

/* Globals */
static struct Job *firstjob = 0;
static struct Job *first_finished_job = 0;
static int jobids = 0;
int max_jobs;


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
    for (p = first_finished_job; p && count < 20; p = p->next)
        finished[count++] = p;
    for (int i = count - 1; i >= 0 && limit; --i, --limit)
        send_view(socket, finished[i]);
    end.type = SNAPSHOT_END;
    end.u.max_slots = max_slots;
    send_msg(socket, &end);
}

void s_wait_notice(int socket, int jobid)
{
    struct Job *job = findjob(jobid), *p;
    struct msg end = {0};
    char line[512], timing[128], cpus[32];
    struct Job_view queued;
    int ahead = 0;
    if (!job || job->state != QUEUED || job->kill_requested) return;
    for (p = firstjob; p && p != job; p = p->next) ++ahead;
    job_view(job, &queued);
    view_cpus(&queued, cpus, sizeof(cpus));
    snprintf(line, sizeof(line), "id=%d queued, cpus=%s, %d ahead\n",
             jobid, cpus, ahead);
    send_list_line(socket, line);
    for (p = firstjob; p; p = p->next) {
        struct Job_view view;
        if (p->state != RUNNING) continue;
        job_view(p, &view);
        view_fields(&view, timing, sizeof(timing));
        view_cpus(&view, cpus, sizeof(cpus));
        snprintf(line, sizeof(line), "  running id=%d dir=%s cpus=%s %s\n",
                 p->jobid, view.label, cpus, timing);
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
int s_newjob(int s, struct msg *m)
{
    struct Job *p;
    int res;

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
    p->pid = 0;
    p->kill_requested = 0;
    p->should_keep_finished = p->state != HOLDING_CLIENT;
    p->label = 0;

    if (recv_bytes(s, (char *)&p->estimate, sizeof(p->estimate)) != (int)sizeof(p->estimate))
        goto disconnected;

    /* load the command */
    p->command = malloc(m->u.newjob.command_size);
    if (p->command == 0)
        error("Cannot allocate memory in s_newjob command_size (%i)",
                m->u.newjob.command_size);
    res = recv_bytes(s, p->command, m->u.newjob.command_size);
    if (res != m->u.newjob.command_size)
        goto disconnected;
    p->command[m->u.newjob.command_size - 1] = '\0';

    /* load the label */
    p->label = 0;
    if (m->u.newjob.label_size > 0)
    {
        char *ptr;
        ptr = (char *) malloc(m->u.newjob.label_size);
        if (ptr == 0)
            error("Cannot allocate memory in s_newjob label_size(%i)",
                    m->u.newjob.label_size);
        p->label = ptr;
        res = recv_bytes(s, ptr, m->u.newjob.label_size);
        if (res != m->u.newjob.label_size)
            goto disconnected;
    }

    return p->jobid;
disconnected:
    {
        struct Result result = {0};
        p->should_keep_finished = 0;
        result.errorlevel = 125;
        job_finished(&result, p->jobid);
    }
    return -1;
}

/* -1 if no one should be run. */
int next_run_job()
{
    struct Job *p;

    const int free_slots = max_slots - busy_slots;

    if (free_slots <= 0)
        return -1;

    /* If there are no jobs to run... */
    if (firstjob == 0)
        return -1;

    /* Look for a runnable task */
    p = firstjob;
    while(p != 0)
    {
        if (p->state == QUEUED)
        {
            if (p->kill_requested) return -1;
            if (free_slots >= p->num_slots)
            {
                busy_slots = busy_slots + p->num_slots;
                return p->jobid;
            }
            return -1; /* Strict FIFO: never bypass the first queued job. */
        }
        p = p->next;
    }

    return -1;
}

/* junban keeps the last 20 finished jobs. */
static int get_max_finished_jobs()
{
    return 20;
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
    if (p->state == RUNNING)
        busy_slots = busy_slots - p->num_slots;

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
    m.u.max_slots = max_slots;

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

