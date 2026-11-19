/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2013  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
enum
{
    PROTOCOL_VERSION=736
};

/* Retired message numbers and fields are reserved to preserve the 40-byte
 * frame. No retired request has a handler. Version 734 adds the job tag
 * and the detached job's log path to the NEWJOB payload and to Job_view.
 * Version 735 adds the GPU count of a job and of the queue. Version 736
 * widens the Job_view command to 256 bytes and adds its wall-clock end. */
enum msg_types
{
    KILL_SERVER,
    NEWJOB,
    NEWJOB_OK,
    RUNJOB,
    RUNJOB_OK,
    ENDJOB,
    RESERVED_LIST,
    LIST_LINE,
    RESERVED_CLEAR_FINISHED,
    RESERVED_ASK_OUTPUT,
    RESERVED_ANSWER_OUTPUT,
    RESERVED_REMOVEJOB,
    RESERVED_REMOVEJOB_OK,
    RESERVED_WAITJOB,
    RESERVED_WAIT_RUNNING_JOB,
    RESERVED_WAITJOB_OK,
    RESERVED_URGENT,
    RESERVED_URGENT_OK,
    RESERVED_GET_STATE,
    RESERVED_ANSWER_STATE,
    RESERVED_SWAP_JOBS,
    RESERVED_SWAP_JOBS_OK,
    RESERVED_INFO,
    RESERVED_INFO_DATA,
    RESERVED_SET_MAX_SLOTS,
    GET_MAX_SLOTS,
    GET_MAX_SLOTS_OK,
    GET_VERSION,
    VERSION,
    NEWJOB_NOK,
    KILL_JOB,
    KILL_JOB_OK,
    JOB_REMOVED,
    RUNJOB_ACK,
    SNAPSHOT,
    SNAPSHOT_ROW,
    SNAPSHOT_END,
    WAIT_NOTICE,
    NOTICE_END,
    JOB_INFO
};

enum Request
{
    c_QUEUE, c_KILL_SERVER, c_LIST, c_KILL_JOB, c_WATCH, c_KILL_TAG, c_WAIT
};

struct Command_line {
    enum Request request;
    int max_slots; /* How many jobs to run at once */
    int jobid; /* When queuing a job, main.c will fill it automatically from
                  the server answer to NEWJOB */
    struct {
        char **array;
        int num;
    } command;
    char *label;
    int num_slots; /* Zero means all slots, unspecified by caller. */
    int num_gpus; /* Zero, the default, means the job uses no GPU. */
    int gpus_given;
    int max_gpus;
    unsigned long long estimate;
    int list_all;
    int list_summary;
    /* Job selection for ls: a job must match one value of each given option. */
    int *ids;
    int num_ids;
    char **dirs;
    int num_dirs;
    char **tags;
    int num_tags;
    char *tag; /* The tag of a new job, or NULL. */
    int detach;
    int verbose;
    unsigned long long timeout; /* Seconds; zero waits forever. */
    char *log_prefix; /* Log directory of a detached job, ending in '/'. */
};

enum Process_type {
    CLIENT,
    SERVER
};

extern struct Command_line command_line;
extern int server_socket;
extern enum Process_type process_type;

struct msg;

enum Jobstate
{
    QUEUED,
    RUNNING,
    FINISHED,
    RESERVED_SKIPPED,
    HOLDING_CLIENT
};

struct msg
{
    enum msg_types type;

    union
    {
        struct {
            int command_size;
            int log_size;
            int reserved_should_keep_finished;
            int label_size;
            int tag_size;
            int num_gpus;
            int reserved_depend_on;
            int reserved_wait_enqueuing;
            int num_slots;
        } newjob;
        struct {
            int reserved_ofilename_size;
            int reserved_store_output;
            int pid;
        } output;
        int jobid;
        struct Result {
            int errorlevel;
            int died_by_signal;
            int signal;
            float reserved_user_ms;
            float reserved_system_ms;
            float reserved_real_ms;
            int reserved_skipped;
        } result;
        int size;
        int reserved_last_errorlevel;
        int max_slots;
        /* SNAPSHOT_END and GET_MAX_SLOTS_OK: max_slots aliases u.max_slots. */
        struct {
            int max_slots;
            int next_jobid;
            int max_gpus;
        } snapshot_end;
        int version;
    } u;
};

struct Job
{
    struct Job *next;
    int jobid;
    char *command;
    enum Jobstate state;
    struct Result result;
    int should_keep_finished;
    int pid;
    char *label;
    char *tag;
    char *log;
    int num_slots;
    int num_gpus;
    int cpus_unspecified;
    int kill_requested;
    int client_lost;
    unsigned long long estimate;
    double queued_at, started_at, finished_at;
    long long finished_wall; /* Wall-clock end, captured once. */
    int finish_pending;
    int stop_kill_sent;
    double stop_deadline;
};

/* Bounded display fields keep control characters and unbounded commands off
 * terminals. The server's full original command and label remain in Job. */
struct Job_view {
    int jobid, state, cpus, gpus, unspecified, killed, client_lost, status, signal;
    double elapsed, waited;
    unsigned long long estimate;
    long long ended; /* Wall-clock end, in seconds since the epoch; 0 if none. */
    char label[128];
    char tag[64];
    char log[512];
    char command[257];
};

struct Snapshot {
    int busy, total, busy_gpus, total_gpus, running, queued;
    int next_jobid; /* The ID the next job gets, read with the same listing. */
    size_t count;
    struct Job_view *jobs;
};

double monotime(void);
void format_duration(char *dst, size_t size, unsigned long long seconds);
void job_view(const struct Job *job, struct Job_view *view);
void fit_columns(char *dst, size_t size, const char *src, int columns, int pad);
int text_columns(const char *src);
void view_cpus(const struct Job_view *view, char *dst, size_t size);
void view_fields(const struct Job_view *view, char *dst, size_t size);
void view_status(const struct Job_view *view, char *dst, size_t size);
void print_summary(const struct Snapshot *snapshot);
int job_selected(const struct Job_view *view);
int print_snapshot(const struct Snapshot *snapshot);
int fetch_snapshot(struct Snapshot *snapshot);
void s_snapshot(int socket, int all);
void s_job_info(int socket, int jobid);
void s_wait_notice(int socket, int jobid);
void c_watch(void);
int s_has_jobs(void);
void prevent_sleep(int active);
void stop_sleep_prevention(void);
int process_group_alive(int pgid);
int s_needs_poll(void);
void s_poll_jobs(void);
void s_end_job(const struct Result *result, int jobid);

/* client.c */
char *build_command_string();
void c_new_job();
int c_wait_newjob_ok();
int c_wait_server_commands();
void c_check_version();
void c_send_runjob_ok(int pid);
void c_shutdown_server();
void c_kill_job();
int c_kill_tag(void);
char *c_log_prefix(void);
void c_detach(void);
void c_wait_prepare(void);
int c_wait(void);
void c_get_max_slots();

/* jobs.c */
void s_mark_job_running(int jobid);
const char * jstate2string(enum Jobstate s);
int s_newjob(int s, struct msg *m);
int next_run_job();
int job_is_holding_client(int jobid);
void job_finished(const struct Result *result, int jobid);
void s_process_runjob_ok(int jobid, int pid);
int s_kill_job(int jobid);
void s_terminate_job(int jobid);
void s_send_runjob(int s, int jobid);
void s_set_max_slots(int new_max_slots);
void s_get_max_slots(int s);
void dump_jobs_struct(FILE *out);

/* server.c */
void server_main(int notify_fd, char *_path, int ls);
void dump_conns_struct(FILE *out);

/* server_start.c */
void create_socket_path(char **path);
int detect_slots(void);
int try_connect(int s);
void wait_server_up(int fd);
void notify_parent(int fd);
int ensure_server_up();

/* execute.c */
int run_job(struct Result *res);

/* error.c */
_Noreturn void error(const char *str, ...);
void warning(const char *str, ...);

/* signals.c */
void ignore_sigpipe();
void restore_sigmask();
void sigint_handler(int s);
void block_sigint();
void unblock_sigint_and_install_handler();

/* msg.c */
void send_bytes(const int fd, const char *data, int bytes);
int recv_bytes(const int fd, char *data, int bytes);
void send_msg(const int fd, const struct msg *m);
int recv_msg(const int fd, struct msg *m);

