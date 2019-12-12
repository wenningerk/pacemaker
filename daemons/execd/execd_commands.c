/*
 * Copyright 2012-2019 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>

#include <glib.h>

#undef PCMK__TIME_USE_CGT
#if HAVE_DECL_CLOCK_MONOTONIC && defined(CLOCK_MONOTONIC) \
    && !defined(PCMK_TIME_EMERGENCY_CGT)
#define PCMK__TIME_USE_CGT
#endif

#ifdef PCMK__TIME_USE_CGT
#  include <time.h>  /* clock_gettime */
#elif defined(HAVE_SYS_TIMEB_H)
#  include <sys/timeb.h>
#endif

#include <unistd.h>

#include <crm/crm.h>
#include <crm/services.h>
#include <crm/common/mainloop.h>
#include <crm/common/ipc.h>
#include <crm/common/ipcs.h>
#include <crm/msg_xml.h>

#include "pacemaker-execd.h"

#define EXIT_REASON_MAX_LEN 128

GHashTable *rsc_list = NULL;

typedef struct lrmd_cmd_s {
    int timeout;
    guint interval_ms;
    int start_delay;
    int timeout_orig;

    int call_id;
    int exec_rc;
    int lrmd_op_status;

    int call_opts;
    /* Timer ids, must be removed on cmd destruction. */
    int delay_id;
    int stonith_recurring_id;

    int rsc_deleted;

    int service_flags;

    char *client_id;
    char *origin;
    char *rsc_id;
    char *action;
    char *real_action;
    char *exit_reason;
    char *output;
    char *userdata_str;

    /* Recurring and systemd operations may involve more than one executor
     * command per operation, so they need info about the original and the most
     * recent.
     */
#ifdef PCMK__TIME_USE_CGT
    struct timespec t_first_run;   /* Timestamp of when op first ran */
    struct timespec t_run;         /* Timestamp of when op most recently ran */
    struct timespec t_first_queue; /* Timestamp of when op first was queued */
    struct timespec t_queue;       /* Timestamp of when op most recently was queued */
    struct timespec t_rcchange;    /* Timestamp of last rc change */
#elif defined(HAVE_SYS_TIMEB_H)
    struct timeb t_first_run;   /* Timestamp of when op first ran */
    struct timeb t_run;         /* Timestamp of when op most recently ran */
    struct timeb t_first_queue; /* Timestamp of when op first was queued */
    struct timeb t_queue;       /* Timestamp of when op most recently was queued */
    struct timeb t_rcchange;    /* Timestamp of last rc change */
#endif

    int first_notify_sent;
    int last_notify_rc;
    int last_notify_op_status;
    int last_pid;

    GHashTable *params;
} lrmd_cmd_t;

static void cmd_finalize(lrmd_cmd_t * cmd, lrmd_rsc_t * rsc);
static gboolean lrmd_rsc_dispatch(gpointer user_data);
static void cancel_all_recurring(lrmd_rsc_t * rsc, const char *client_id);

static void
log_finished(lrmd_cmd_t * cmd, int exec_time, int queue_time)
{
    char pid_str[32] = { 0, };
    int log_level = LOG_INFO;

    if (cmd->last_pid) {
        snprintf(pid_str, 32, "%d", cmd->last_pid);
    }

    if (safe_str_eq(cmd->action, "monitor")) {
        log_level = LOG_DEBUG;
    }
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
    do_crm_log(log_level,
               "finished - rsc:%s action:%s call_id:%d %s%s exit-code:%d exec-time:%dms queue-time:%dms",
               cmd->rsc_id, cmd->action, cmd->call_id, cmd->last_pid ? "pid:" : "", pid_str,
               cmd->exec_rc, exec_time, queue_time);
#else
    do_crm_log(log_level, "finished - rsc:%s action:%s call_id:%d %s%s exit-code:%d",
               cmd->rsc_id,
               cmd->action, cmd->call_id, cmd->last_pid ? "pid:" : "", pid_str, cmd->exec_rc);
#endif
}

static void
log_execute(lrmd_cmd_t * cmd)
{
    int log_level = LOG_INFO;

    if (safe_str_eq(cmd->action, "monitor")) {
        log_level = LOG_DEBUG;
    }

    do_crm_log(log_level, "executing - rsc:%s action:%s call_id:%d",
               cmd->rsc_id, cmd->action, cmd->call_id);
}

static const char *
normalize_action_name(lrmd_rsc_t * rsc, const char *action)
{
    if (safe_str_eq(action, "monitor") &&
        is_set(pcmk_get_ra_caps(rsc->class), pcmk_ra_cap_status)) {
        return "status";
    }
    return action;
}

static lrmd_rsc_t *
build_rsc_from_xml(xmlNode * msg)
{
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, msg, LOG_ERR);
    lrmd_rsc_t *rsc = NULL;

    rsc = calloc(1, sizeof(lrmd_rsc_t));

    crm_element_value_int(msg, F_LRMD_CALLOPTS, &rsc->call_opts);

    rsc->rsc_id = crm_element_value_copy(rsc_xml, F_LRMD_RSC_ID);
    rsc->class = crm_element_value_copy(rsc_xml, F_LRMD_CLASS);
    rsc->provider = crm_element_value_copy(rsc_xml, F_LRMD_PROVIDER);
    rsc->type = crm_element_value_copy(rsc_xml, F_LRMD_TYPE);
    rsc->work = mainloop_add_trigger(G_PRIORITY_HIGH, lrmd_rsc_dispatch, rsc);
    rsc->st_probe_rc = -ENODEV; // if stonith, initialize to "not running"
    return rsc;
}

static lrmd_cmd_t *
create_lrmd_cmd(xmlNode * msg, crm_client_t * client)
{
    int call_options = 0;
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, msg, LOG_ERR);
    lrmd_cmd_t *cmd = NULL;

    cmd = calloc(1, sizeof(lrmd_cmd_t));

    crm_element_value_int(msg, F_LRMD_CALLOPTS, &call_options);
    cmd->call_opts = call_options;
    cmd->client_id = strdup(client->id);

    crm_element_value_int(msg, F_LRMD_CALLID, &cmd->call_id);
    crm_element_value_ms(rsc_xml, F_LRMD_RSC_INTERVAL, &cmd->interval_ms);
    crm_element_value_int(rsc_xml, F_LRMD_TIMEOUT, &cmd->timeout);
    crm_element_value_int(rsc_xml, F_LRMD_RSC_START_DELAY, &cmd->start_delay);
    cmd->timeout_orig = cmd->timeout;

    cmd->origin = crm_element_value_copy(rsc_xml, F_LRMD_ORIGIN);
    cmd->action = crm_element_value_copy(rsc_xml, F_LRMD_RSC_ACTION);
    cmd->userdata_str = crm_element_value_copy(rsc_xml, F_LRMD_RSC_USERDATA_STR);
    cmd->rsc_id = crm_element_value_copy(rsc_xml, F_LRMD_RSC_ID);

    cmd->params = xml2list(rsc_xml);

    if (safe_str_eq(g_hash_table_lookup(cmd->params, "CRM_meta_on_fail"), "block")) {
        crm_debug("Setting flag to leave pid group on timeout and only kill action pid for " CRM_OP_FMT,
                  cmd->rsc_id, cmd->action, cmd->interval_ms);
        cmd->service_flags |= SVC_ACTION_LEAVE_GROUP;
    }
    return cmd;
}

static void
stop_recurring_timer(lrmd_cmd_t *cmd)
{
    if (cmd) {
        if (cmd->stonith_recurring_id) {
            g_source_remove(cmd->stonith_recurring_id);
        }
        cmd->stonith_recurring_id = 0;
    }
}

static void
free_lrmd_cmd(lrmd_cmd_t * cmd)
{
    stop_recurring_timer(cmd);
    if (cmd->delay_id) {
        g_source_remove(cmd->delay_id);
    }
    if (cmd->params) {
        g_hash_table_destroy(cmd->params);
    }
    free(cmd->origin);
    free(cmd->action);
    free(cmd->real_action);
    free(cmd->userdata_str);
    free(cmd->rsc_id);
    free(cmd->output);
    free(cmd->exit_reason);
    free(cmd->client_id);
    free(cmd);
}

static gboolean
stonith_recurring_op_helper(gpointer data)
{
    lrmd_cmd_t *cmd = data;
    lrmd_rsc_t *rsc;

    cmd->stonith_recurring_id = 0;

    if (!cmd->rsc_id) {
        return FALSE;
    }

    rsc = g_hash_table_lookup(rsc_list, cmd->rsc_id);

    CRM_ASSERT(rsc != NULL);
    /* take it out of recurring_ops list, and put it in the pending ops
     * to be executed */
    rsc->recurring_ops = g_list_remove(rsc->recurring_ops, cmd);
    rsc->pending_ops = g_list_append(rsc->pending_ops, cmd);
#ifdef PCMK__TIME_USE_CGT
    clock_gettime(CLOCK_MONOTONIC, &cmd->t_queue);
    if (cmd->t_first_queue.tv_sec == 0) {
        cmd->t_first_queue = cmd->t_queue;
    }
#elif defined(HAVE_SYS_TIMEB_H)
    ftime(&cmd->t_queue);
    if (cmd->t_first_queue.time == 0) {
        cmd->t_first_queue = cmd->t_queue;
    }
#endif
    mainloop_set_trigger(rsc->work);

    return FALSE;
}

static inline void
start_recurring_timer(lrmd_cmd_t *cmd)
{
    if (cmd && (cmd->interval_ms > 0)) {
        cmd->stonith_recurring_id = g_timeout_add(cmd->interval_ms,
                                                  stonith_recurring_op_helper,
                                                  cmd);
    }
}

static gboolean
start_delay_helper(gpointer data)
{
    lrmd_cmd_t *cmd = data;
    lrmd_rsc_t *rsc = NULL;

    cmd->delay_id = 0;
    rsc = cmd->rsc_id ? g_hash_table_lookup(rsc_list, cmd->rsc_id) : NULL;

    if (rsc) {
        mainloop_set_trigger(rsc->work);
    }

    return FALSE;
}

static gboolean
merge_recurring_duplicate(lrmd_rsc_t * rsc, lrmd_cmd_t * cmd)
{
    GListPtr gIter = NULL;
    lrmd_cmd_t * dup = NULL;
    gboolean dup_pending = FALSE;

    if (cmd->interval_ms == 0) {
        return 0;
    }

    for (gIter = rsc->pending_ops; gIter != NULL; gIter = gIter->next) {
        dup = gIter->data;
        if (safe_str_eq(cmd->action, dup->action)
            && (cmd->interval_ms == dup->interval_ms)) {
            dup_pending = TRUE;
            goto merge_dup;
        }
    }

    /* if dup is in recurring_ops list, that means it has already executed
     * and is in the interval loop. we can't just remove it in this case. */
    for (gIter = rsc->recurring_ops; gIter != NULL; gIter = gIter->next) {
        dup = gIter->data;
        if (safe_str_eq(cmd->action, dup->action)
            && (cmd->interval_ms == dup->interval_ms)) {
            goto merge_dup;
        }
    }

    return FALSE;
merge_dup:


    /* This should not occur. If it does, we need to investigate how something
     * like this is possible in the controller.
     */
    crm_warn("Duplicate recurring op entry detected (" CRM_OP_FMT "), merging with previous op entry",
            rsc->rsc_id,
            normalize_action_name(rsc, dup->action),
            dup->interval_ms);

    /* merge */
    dup->first_notify_sent = 0;
    free(dup->userdata_str);
    dup->userdata_str = cmd->userdata_str;
    cmd->userdata_str = NULL;
    dup->call_id = cmd->call_id;

    if (safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_STONITH)) {
        /* if we are waiting for the next interval, kick it off now */
        if (dup_pending == TRUE) {
            stop_recurring_timer(cmd);
            stonith_recurring_op_helper(cmd);
        }

    } else if (dup_pending == FALSE) {
        /* if we've already handed this to the service lib, kick off an early execution */
        services_action_kick(rsc->rsc_id,
                             normalize_action_name(rsc, dup->action),
                             dup->interval_ms);
    }
    free_lrmd_cmd(cmd);

    return TRUE;
}

static void
schedule_lrmd_cmd(lrmd_rsc_t * rsc, lrmd_cmd_t * cmd)
{
    gboolean dup_processed = FALSE;
    CRM_CHECK(cmd != NULL, return);
    CRM_CHECK(rsc != NULL, return);

    crm_trace("Scheduling %s on %s", cmd->action, rsc->rsc_id);

    dup_processed = merge_recurring_duplicate(rsc, cmd);
    if (dup_processed) {
        /* duplicate recurring cmd found, cmds merged */
        return;
    }

    /* The controller expects the executor to automatically cancel
     * recurring operations before a resource stops.
     */
    if (safe_str_eq(cmd->action, "stop")) {
        cancel_all_recurring(rsc, NULL);
    }

    rsc->pending_ops = g_list_append(rsc->pending_ops, cmd);
#ifdef PCMK__TIME_USE_CGT
    clock_gettime(CLOCK_MONOTONIC, &cmd->t_queue);
    if (cmd->t_first_queue.tv_sec == 0) {
        cmd->t_first_queue = cmd->t_queue;
    }
#elif defined(HAVE_SYS_TIMEB_H)
    ftime(&cmd->t_queue);
    if (cmd->t_first_queue.time == 0) {
        cmd->t_first_queue = cmd->t_queue;
    }
#endif
    mainloop_set_trigger(rsc->work);

    if (cmd->start_delay) {
        cmd->delay_id = g_timeout_add(cmd->start_delay, start_delay_helper, cmd);
    }
}

static xmlNode *
create_lrmd_reply(const char *origin, int rc, int call_id)
{
    xmlNode *reply = create_xml_node(NULL, T_LRMD_REPLY);

    crm_xml_add(reply, F_LRMD_ORIGIN, origin);
    crm_xml_add_int(reply, F_LRMD_RC, rc);
    crm_xml_add_int(reply, F_LRMD_CALLID, call_id);
    return reply;
}

static void
send_client_notify(gpointer key, gpointer value, gpointer user_data)
{
    xmlNode *update_msg = user_data;
    crm_client_t *client = value;
    int rc;
    int log_level = LOG_WARNING;
    const char *msg = NULL;

    CRM_CHECK(client != NULL, return);
    if (client->name == NULL) {
        crm_trace("Skipping notification to client without name");
        return;
    }

    rc = lrmd_server_send_notify(client, update_msg);
    if (rc > 0) {
        return; // success
    }

    switch (rc) {
        case 0:
            msg = "no data sent";
            break;

        case -ENOTCONN:
        case -EPIPE: // Client exited without waiting for notification
            log_level = LOG_INFO;
            msg = "Disconnected";
            break;

        default:
            msg = pcmk_strerror(rc);
            break;
    }
    do_crm_log(log_level,
               "Could not notify client %s/%s: %s " CRM_XS " rc=%d",
               client->name, client->id, msg, rc);
}

#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
/*!
 * \internal
 * \brief Return difference between two times in milliseconds
 *
 * \param[in] now  More recent time (or NULL to use current time)
 * \param[in] old  Earlier time
 *
 * \return milliseconds difference (or 0 if old is NULL or has tv_sec/time zero)
 *
 * \note Can overflow on 32bit machines when the differences is around
 *       24 days or more.
 */
static int
#ifdef PCMK__TIME_USE_CGT
time_diff_ms(struct timespec *now, struct timespec *old)
{
    struct timespec local_now = { 0, };
#else
time_diff_ms(struct timeb *now, struct timeb *old)
{
    struct timeb local_now = { 0, };
#endif

    if (now == NULL) {
#ifdef PCMK__TIME_USE_CGT
        clock_gettime(CLOCK_MONOTONIC, &local_now);
#else
        ftime(&local_now);
#endif
        now = &local_now;
    }
    if ((old == NULL)
#ifdef PCMK__TIME_USE_CGT
            || (old->tv_sec == 0)) {
#else
            || (old->time == 0)) {
#endif
        return 0;
    }
#ifdef PCMK__TIME_USE_CGT
    return (now->tv_sec - old->tv_sec) * 1000
            + (now->tv_nsec - old->tv_nsec) / 1000;
#else
    return (now->time - old->time) * 1000 + now->millitm - old->millitm;
#endif
}

/*!
 * \internal
 * \brief Reset a command's operation times to their original values.
 *
 * Reset a command's run and queued timestamps to the timestamps of the original
 * command, so we report the entire time since then and not just the time since
 * the most recent command (for recurring and systemd operations).
 *
 * /param[in] cmd  Executor command object to reset
 *
 * /note It's not obvious what the queued time should be for a systemd
 * start/stop operation, which might go like this:
 *   initial command queued 5ms, runs 3s
 *   monitor command queued 10ms, runs 10s
 *   monitor command queued 10ms, runs 10s
 * Is the queued time for that operation 5ms, 10ms or 25ms? The current
 * implementation will report 5ms. If it's 25ms, then we need to
 * subtract 20ms from the total exec time so as not to count it twice.
 * We can implement that later if it matters to anyone ...
 */
static void
cmd_original_times(lrmd_cmd_t * cmd)
{
    cmd->t_run = cmd->t_first_run;
    cmd->t_queue = cmd->t_first_queue;
}
#endif

static void
send_cmd_complete_notify(lrmd_cmd_t * cmd)
{
    int exec_time = 0;
    int queue_time = 0;
    xmlNode *notify = NULL;

#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
    exec_time = time_diff_ms(NULL, &cmd->t_run);
    queue_time = time_diff_ms(&cmd->t_run, &cmd->t_queue);
#endif

    log_finished(cmd, exec_time, queue_time);

    /* if the first notify result for a cmd has already been sent earlier, and the
     * the option to only send notifies on result changes is set. Check to see
     * if the last result is the same as the new one. If so, suppress this update */
    if (cmd->first_notify_sent && (cmd->call_opts & lrmd_opt_notify_changes_only)) {
        if (cmd->last_notify_rc == cmd->exec_rc &&
            cmd->last_notify_op_status == cmd->lrmd_op_status) {

            /* only send changes */
            return;
        }

    }

    cmd->first_notify_sent = 1;
    cmd->last_notify_rc = cmd->exec_rc;
    cmd->last_notify_op_status = cmd->lrmd_op_status;

    notify = create_xml_node(NULL, T_LRMD_NOTIFY);

    crm_xml_add(notify, F_LRMD_ORIGIN, __FUNCTION__);
    crm_xml_add_int(notify, F_LRMD_TIMEOUT, cmd->timeout);
    crm_xml_add_ms(notify, F_LRMD_RSC_INTERVAL, cmd->interval_ms);
    crm_xml_add_int(notify, F_LRMD_RSC_START_DELAY, cmd->start_delay);
    crm_xml_add_int(notify, F_LRMD_EXEC_RC, cmd->exec_rc);
    crm_xml_add_int(notify, F_LRMD_OP_STATUS, cmd->lrmd_op_status);
    crm_xml_add_int(notify, F_LRMD_CALLID, cmd->call_id);
    crm_xml_add_int(notify, F_LRMD_RSC_DELETED, cmd->rsc_deleted);

#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
#ifdef PCMK__TIME_USE_CGT
    crm_xml_add_ll(notify, F_LRMD_RSC_RUN_TIME, (long long) cmd->t_run.tv_sec);
    crm_xml_add_ll(notify, F_LRMD_RSC_RCCHANGE_TIME, (long long) cmd->t_rcchange.tv_sec);
#else
    crm_xml_add_ll(notify, F_LRMD_RSC_RUN_TIME, (long long) cmd->t_run.time);
    crm_xml_add_ll(notify, F_LRMD_RSC_RCCHANGE_TIME, (long long) cmd->t_rcchange.time);
#endif
    crm_xml_add_int(notify, F_LRMD_RSC_EXEC_TIME, exec_time);
    crm_xml_add_int(notify, F_LRMD_RSC_QUEUE_TIME, queue_time);
#endif

    crm_xml_add(notify, F_LRMD_OPERATION, LRMD_OP_RSC_EXEC);
    crm_xml_add(notify, F_LRMD_RSC_ID, cmd->rsc_id);
    if(cmd->real_action) {
        crm_xml_add(notify, F_LRMD_RSC_ACTION, cmd->real_action);
    } else {
        crm_xml_add(notify, F_LRMD_RSC_ACTION, cmd->action);
    }
    crm_xml_add(notify, F_LRMD_RSC_USERDATA_STR, cmd->userdata_str);
    crm_xml_add(notify, F_LRMD_RSC_OUTPUT, cmd->output);
    crm_xml_add(notify, F_LRMD_RSC_EXIT_REASON, cmd->exit_reason);

    if (cmd->params) {
        char *key = NULL;
        char *value = NULL;
        GHashTableIter iter;

        xmlNode *args = create_xml_node(notify, XML_TAG_ATTRS);

        g_hash_table_iter_init(&iter, cmd->params);
        while (g_hash_table_iter_next(&iter, (gpointer *) & key, (gpointer *) & value)) {
            hash2smartfield((gpointer) key, (gpointer) value, args);
        }
    }
    if (cmd->client_id && (cmd->call_opts & lrmd_opt_notify_orig_only)) {
        crm_client_t *client = crm_client_get_by_id(cmd->client_id);

        if (client) {
            send_client_notify(client->id, client, notify);
        }
    } else if (client_connections != NULL) {
        g_hash_table_foreach(client_connections, send_client_notify, notify);
    }

    free_xml(notify);
}

static void
send_generic_notify(int rc, xmlNode * request)
{
    if (client_connections != NULL) {
        int call_id = 0;
        xmlNode *notify = NULL;
        xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, request, LOG_ERR);
        const char *rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);
        const char *op = crm_element_value(request, F_LRMD_OPERATION);

        crm_element_value_int(request, F_LRMD_CALLID, &call_id);

        notify = create_xml_node(NULL, T_LRMD_NOTIFY);
        crm_xml_add(notify, F_LRMD_ORIGIN, __FUNCTION__);
        crm_xml_add_int(notify, F_LRMD_RC, rc);
        crm_xml_add_int(notify, F_LRMD_CALLID, call_id);
        crm_xml_add(notify, F_LRMD_OPERATION, op);
        crm_xml_add(notify, F_LRMD_RSC_ID, rsc_id);

        g_hash_table_foreach(client_connections, send_client_notify, notify);

        free_xml(notify);
    }
}

static void
cmd_reset(lrmd_cmd_t * cmd)
{
    cmd->lrmd_op_status = 0;
    cmd->last_pid = 0;
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
    memset(&cmd->t_run, 0, sizeof(cmd->t_run));
    memset(&cmd->t_queue, 0, sizeof(cmd->t_queue));
#endif
    free(cmd->exit_reason);
    cmd->exit_reason = NULL;
    free(cmd->output);
    cmd->output = NULL;

}

static void
cmd_finalize(lrmd_cmd_t * cmd, lrmd_rsc_t * rsc)
{
    crm_trace("Resource operation rsc:%s action:%s completed (%p %p)", cmd->rsc_id, cmd->action,
              rsc ? rsc->active : NULL, cmd);

    if (rsc && (rsc->active == cmd)) {
        rsc->active = NULL;
        mainloop_set_trigger(rsc->work);
    }

    if (!rsc) {
        cmd->rsc_deleted = 1;
    }

    /* reset original timeout so client notification has correct information */
    cmd->timeout = cmd->timeout_orig;

    send_cmd_complete_notify(cmd);

    if (cmd->interval_ms && (cmd->lrmd_op_status == PCMK_LRM_OP_CANCELLED)) {
        if (rsc) {
            rsc->recurring_ops = g_list_remove(rsc->recurring_ops, cmd);
            rsc->pending_ops = g_list_remove(rsc->pending_ops, cmd);
        }
        free_lrmd_cmd(cmd);
    } else if (cmd->interval_ms == 0) {
        if (rsc) {
            rsc->pending_ops = g_list_remove(rsc->pending_ops, cmd);
        }
        free_lrmd_cmd(cmd);
    } else {
        /* Clear all the values pertaining just to the last iteration of a recurring op. */
        cmd_reset(cmd);
    }
}

static int
ocf2uniform_rc(int rc)
{
    if (rc < 0 || rc > PCMK_OCF_FAILED_MASTER) {
        return PCMK_OCF_UNKNOWN_ERROR;
    }

    return rc;
}

static int
stonith2uniform_rc(const char *action, int rc)
{
    switch (rc) {
        case pcmk_ok:
            rc = PCMK_OCF_OK;
            break;

        case -ENODEV:
            /* This should be possible only for probes in practice, but
             * interpret for all actions to be safe.
             */
            if (safe_str_eq(action, "monitor")) {
                rc = PCMK_OCF_NOT_RUNNING;
            } else if (safe_str_eq(action, "stop")) {
                rc = PCMK_OCF_OK;
            } else {
                rc = PCMK_OCF_NOT_INSTALLED;
            }
            break;

        case -EOPNOTSUPP:
            rc = PCMK_OCF_UNIMPLEMENT_FEATURE;
            break;

        case -ETIME:
        case -ETIMEDOUT:
            rc = PCMK_OCF_TIMEOUT;
            break;

        default:
            rc = PCMK_OCF_UNKNOWN_ERROR;
            break;
    }
    return rc;
}

#if SUPPORT_NAGIOS
static int
nagios2uniform_rc(const char *action, int rc)
{
    if (rc < 0) {
        return PCMK_OCF_UNKNOWN_ERROR;
    }

    switch (rc) {
        case NAGIOS_STATE_OK:
            return PCMK_OCF_OK;
        case NAGIOS_INSUFFICIENT_PRIV:
            return PCMK_OCF_INSUFFICIENT_PRIV;
        case NAGIOS_NOT_INSTALLED:
            return PCMK_OCF_NOT_INSTALLED;
        case NAGIOS_STATE_WARNING:
        case NAGIOS_STATE_CRITICAL:
        case NAGIOS_STATE_UNKNOWN:
        case NAGIOS_STATE_DEPENDENT:
        default:
            return PCMK_OCF_UNKNOWN_ERROR;
    }

    return PCMK_OCF_UNKNOWN_ERROR;
}
#endif

static int
get_uniform_rc(const char *standard, const char *action, int rc)
{
    if (safe_str_eq(standard, PCMK_RESOURCE_CLASS_OCF)) {
        return ocf2uniform_rc(rc);
    } else if (safe_str_eq(standard, PCMK_RESOURCE_CLASS_STONITH)) {
        return stonith2uniform_rc(action, rc);
    } else if (safe_str_eq(standard, PCMK_RESOURCE_CLASS_SYSTEMD)) {
        return rc;
    } else if (safe_str_eq(standard, PCMK_RESOURCE_CLASS_UPSTART)) {
        return rc;
#if SUPPORT_NAGIOS
    } else if (safe_str_eq(standard, PCMK_RESOURCE_CLASS_NAGIOS)) {
        return nagios2uniform_rc(action, rc);
#endif
    } else {
        return services_get_ocf_exitcode(action, rc);
    }
}

static int
action_get_uniform_rc(svc_action_t * action)
{
    lrmd_cmd_t *cmd = action->cb_data;
    return get_uniform_rc(action->standard, cmd->action, action->rc);
}

void
notify_of_new_client(crm_client_t *new_client)
{
    crm_client_t *client = NULL;
    GHashTableIter iter;
    xmlNode *notify = NULL;
    char *key = NULL;

    notify = create_xml_node(NULL, T_LRMD_NOTIFY);
    crm_xml_add(notify, F_LRMD_ORIGIN, __FUNCTION__);
    crm_xml_add(notify, F_LRMD_OPERATION, LRMD_OP_NEW_CLIENT);

    g_hash_table_iter_init(&iter, client_connections);
    while (g_hash_table_iter_next(&iter, (gpointer *) & key, (gpointer *) & client)) {

        if (safe_str_eq(client->id, new_client->id)) {
            continue;
        }

        send_client_notify((gpointer) key, (gpointer) client, (gpointer) notify);
    }
    free_xml(notify);
}

static char *
parse_exit_reason(const char *output)
{
    const char *cur = NULL;
    const char *last = NULL;
    static int cookie_len = 0;
    char *eol = NULL;
    size_t reason_len = EXIT_REASON_MAX_LEN;

    if (output == NULL) {
        return NULL;
    }

    if (!cookie_len) {
        cookie_len = strlen(PCMK_OCF_REASON_PREFIX);
    }

    cur = strstr(output, PCMK_OCF_REASON_PREFIX);
    for (; cur != NULL; cur = strstr(cur, PCMK_OCF_REASON_PREFIX)) {
        /* skip over the cookie delimiter string */
        cur += cookie_len;
        last = cur;
    }
    if (last == NULL) {
        return NULL;
    }

    // Truncate everything after a new line, and limit reason string size
    eol = strchr(last, '\n');
    if (eol) {
        reason_len = QB_MIN(reason_len, eol - last);
    }
    return strndup(last, reason_len);
}

void
client_disconnect_cleanup(const char *client_id)
{
    GHashTableIter iter;
    lrmd_rsc_t *rsc = NULL;
    char *key = NULL;

    g_hash_table_iter_init(&iter, rsc_list);
    while (g_hash_table_iter_next(&iter, (gpointer *) & key, (gpointer *) & rsc)) {
        if (rsc->call_opts & lrmd_opt_drop_recurring) {
            /* This client is disconnecting, drop any recurring operations
             * it may have initiated on the resource */
            cancel_all_recurring(rsc, client_id);
        }
    }
}

static void
action_complete(svc_action_t * action)
{
    lrmd_rsc_t *rsc;
    lrmd_cmd_t *cmd = action->cb_data;
    const char *rclass = NULL;

#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
    bool goagain = false;
#endif

    if (!cmd) {
        crm_err("Completed executor action (%s) does not match any known operations",
                action->id);
        return;
    }

#ifdef PCMK__TIME_USE_CGT
    if (cmd->exec_rc != action->rc) {
        clock_gettime(CLOCK_MONOTONIC, &cmd->t_rcchange);
    }
#elif defined(HAVE_SYS_TIMEB_H)
    if (cmd->exec_rc != action->rc) {
        ftime(&cmd->t_rcchange);
    }
#endif

    cmd->last_pid = action->pid;
    cmd->exec_rc = action_get_uniform_rc(action);
    cmd->lrmd_op_status = action->status;
    rsc = cmd->rsc_id ? g_hash_table_lookup(rsc_list, cmd->rsc_id) : NULL;

    if (rsc && safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_SERVICE)) {
        rclass = resources_find_service_class(rsc->type);
    } else if(rsc) {
        rclass = rsc->class;
    }

    if (safe_str_eq(rclass, PCMK_RESOURCE_CLASS_SYSTEMD)) {
        if(cmd->exec_rc == PCMK_OCF_OK && safe_str_eq(cmd->action, "start")) {
            /* systemd I curse thee!
             *
             * systemd returns from start actions after the start _begins_
             * not after it completes.
             *
             * So we have to jump through a few hoops so that we don't
             * report 'complete' to the rest of pacemaker until, you know,
             * it's actually done.
             */
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
            goagain = true;
#endif
            cmd->real_action = cmd->action;
            cmd->action = strdup("monitor");

        } else if(cmd->exec_rc == PCMK_OCF_OK && safe_str_eq(cmd->action, "stop")) {
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
            goagain = true;
#endif
            cmd->real_action = cmd->action;
            cmd->action = strdup("monitor");

        } else if(cmd->real_action) {
            /* Ok, so this is the follow up monitor action to check if start actually completed */
            if(cmd->lrmd_op_status == PCMK_LRM_OP_DONE && cmd->exec_rc == PCMK_OCF_PENDING) {
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
                goagain = true;
#endif
            } else if(cmd->exec_rc == PCMK_OCF_OK && safe_str_eq(cmd->real_action, "stop")) {
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
                goagain = true;
#endif

            } else {
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
                int time_sum = time_diff_ms(NULL, &cmd->t_first_run);
                int timeout_left = cmd->timeout_orig - time_sum;

                crm_debug("%s %s is now complete (elapsed=%dms, remaining=%dms): %s (%d)",
                          cmd->rsc_id, cmd->real_action, time_sum, timeout_left, services_ocf_exitcode_str(cmd->exec_rc), cmd->exec_rc);
                cmd_original_times(cmd);
#endif

                // Monitors may return "not running", but start/stop shouldn't
                if ((cmd->lrmd_op_status == PCMK_LRM_OP_DONE)
                    && (cmd->exec_rc == PCMK_OCF_NOT_RUNNING)) {

                    if (safe_str_eq(cmd->real_action, "start")) {
                        cmd->exec_rc = PCMK_OCF_UNKNOWN_ERROR;
                    } else if (safe_str_eq(cmd->real_action, "stop")) {
                        cmd->exec_rc = PCMK_OCF_OK;
                    }
                }
            }
        }
    }

#if SUPPORT_NAGIOS
    if (rsc && safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_NAGIOS)) {
        if (safe_str_eq(cmd->action, "monitor") &&
            (cmd->interval_ms == 0) && cmd->exec_rc == PCMK_OCF_OK) {
            /* Successfully executed --version for the nagios plugin */
            cmd->exec_rc = PCMK_OCF_NOT_RUNNING;

        } else if (safe_str_eq(cmd->action, "start") && cmd->exec_rc != PCMK_OCF_OK) {
#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
            goagain = true;
#endif
        }
    }
#endif

#if defined(PCMK__TIME_USE_CGT) || defined(HAVE_SYS_TIMEB_H)
    /* Wrapping this section in ifdef implies that systemd resources... */
#ifdef PCMK__TIME_USE_CGT
    /* .... are not fully supported on platforms without CLOCK_MONOTONIC,
       which is most likely contradiction, but for completeness... */
#elif defined(HAVE_SYS_TIMEB_H)
    /* ... are not fully supported on platforms without sys/timeb.h.
       Since timeb is obsolete, we should eventually prefer a clock_gettime()
       implementation (wrapped in its own ifdef) with timeb as a fallback. */
#endif
    if (goagain) {
        int time_sum = time_diff_ms(NULL, &cmd->t_first_run);
        int timeout_left = cmd->timeout_orig - time_sum;
        int delay = cmd->timeout_orig / 10;

        if(delay >= timeout_left && timeout_left > 20) {
            delay = timeout_left/2;
        }

        delay = QB_MIN(2000, delay);
        if (delay < timeout_left) {
            cmd->start_delay = delay;
            cmd->timeout = timeout_left;

            if(cmd->exec_rc == PCMK_OCF_OK) {
                crm_debug("%s %s may still be in progress: re-scheduling (elapsed=%dms, remaining=%dms, start_delay=%dms)",
                          cmd->rsc_id, cmd->real_action, time_sum, timeout_left, delay);

            } else if(cmd->exec_rc == PCMK_OCF_PENDING) {
                crm_info("%s %s is still in progress: re-scheduling (elapsed=%dms, remaining=%dms, start_delay=%dms)",
                         cmd->rsc_id, cmd->action, time_sum, timeout_left, delay);

            } else {
                crm_notice("%s %s failed '%s' (%d): re-scheduling (elapsed=%dms, remaining=%dms, start_delay=%dms)",
                           cmd->rsc_id, cmd->action, services_ocf_exitcode_str(cmd->exec_rc), cmd->exec_rc, time_sum, timeout_left, delay);
            }

            cmd_reset(cmd);
            if(rsc) {
                rsc->active = NULL;
            }
            schedule_lrmd_cmd(rsc, cmd);

            /* Don't finalize cmd, we're not done with it yet */
            return;

        } else {
            crm_notice("Giving up on %s %s (rc=%d): timeout (elapsed=%dms, remaining=%dms)",
                       cmd->rsc_id, cmd->real_action?cmd->real_action:cmd->action, cmd->exec_rc, time_sum, timeout_left);
            cmd->lrmd_op_status = PCMK_LRM_OP_TIMEOUT;
            cmd->exec_rc = PCMK_OCF_TIMEOUT;
            cmd_original_times(cmd);
        }
    }
#endif

    if (action->stderr_data) {
        cmd->output = strdup(action->stderr_data);
        cmd->exit_reason = parse_exit_reason(action->stderr_data);

    } else if (action->stdout_data) {
        cmd->output = strdup(action->stdout_data);
    }

    cmd_finalize(cmd, rsc);
}

/*!
 * \internal
 * \brief Determine operation status of a stonith operation
 *
 * Non-stonith resource operations get their operation status directly from the
 * service library, but the fencer does not have an equivalent, so we must infer
 * an operation status from the fencer API's return code.
 *
 * \param[in] action       Name of action performed on stonith resource
 * \param[in] interval_ms  Action interval
 * \param[in] rc           Action result from fencer
 *
 * \return Operation status corresponding to fencer API return code
 */
static int
stonith_rc2status(const char *action, guint interval_ms, int rc)
{
    int status = PCMK_LRM_OP_DONE;

    switch (rc) {
        case pcmk_ok:
            break;

        case -EOPNOTSUPP:
        case -EPROTONOSUPPORT:
            status = PCMK_LRM_OP_NOTSUPPORTED;
            break;

        case -ETIME:
        case -ETIMEDOUT:
            status = PCMK_LRM_OP_TIMEOUT;
            break;

        case -ENOTCONN:
        case -ECOMM:
            // Couldn't talk to fencer
            status = PCMK_LRM_OP_ERROR;
            break;

        case -ENODEV:
            // The device is not registered with the fencer
            status = PCMK_LRM_OP_ERROR;
            break;

        default:
            break;
    }
    return status;
}

static void
stonith_action_complete(lrmd_cmd_t * cmd, int rc)
{
    // This can be NULL if resource was removed before command completed
    lrmd_rsc_t *rsc = g_hash_table_lookup(rsc_list, cmd->rsc_id);

    cmd->exec_rc = stonith2uniform_rc(cmd->action, rc);

    /* This function may be called with status already set to cancelled, if a
     * pending action was aborted. Otherwise, we need to determine status from
     * the fencer return code.
     */
    if (cmd->lrmd_op_status != PCMK_LRM_OP_CANCELLED) {
        cmd->lrmd_op_status = stonith_rc2status(cmd->action, cmd->interval_ms,
                                                rc);

        // Certain successful actions change the known state of the resource
        if (rsc && (cmd->exec_rc == PCMK_OCF_OK)) {
            if (safe_str_eq(cmd->action, "start")) {
                rsc->st_probe_rc = pcmk_ok; // maps to PCMK_OCF_OK
            } else if (safe_str_eq(cmd->action, "stop")) {
                rsc->st_probe_rc = -ENODEV; // maps to PCMK_OCF_NOT_RUNNING
            }
        }
    }

    /* The recurring timer should not be running at this point in any case, but
     * as a failsafe, stop it if it is.
     */
    stop_recurring_timer(cmd);

    /* Reschedule this command if appropriate. If a recurring command is *not*
     * rescheduled, its status must be PCMK_LRM_OP_CANCELLED, otherwise it will
     * not be removed from recurring_ops by cmd_finalize().
     */
    if (rsc && (cmd->interval_ms > 0)
        && (cmd->lrmd_op_status != PCMK_LRM_OP_CANCELLED)) {
        start_recurring_timer(cmd);
    }

    cmd_finalize(cmd, rsc);
}

static void
lrmd_stonith_callback(stonith_t * stonith, stonith_callback_data_t * data)
{
    stonith_action_complete(data->userdata, data->rc);
}

void
stonith_connection_failed(void)
{
    GHashTableIter iter;
    GList *cmd_list = NULL;
    GList *cmd_iter = NULL;
    lrmd_rsc_t *rsc = NULL;
    char *key = NULL;

    g_hash_table_iter_init(&iter, rsc_list);
    while (g_hash_table_iter_next(&iter, (gpointer *) & key, (gpointer *) & rsc)) {
        if (safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_STONITH)) {
            /* If we registered this fence device, we don't know whether the
             * fencer still has the registration or not. Cause future probes to
             * return PCMK_OCF_UNKNOWN_ERROR until the resource is stopped or
             * started successfully. This is especially important if the
             * controller also went away (possibly due to a cluster layer
             * restart) and won't receive our client notification of any
             * monitors finalized below.
             */
            if (rsc->st_probe_rc == pcmk_ok) {
                rsc->st_probe_rc = pcmk_err_generic;
            }

            if (rsc->active) {
                cmd_list = g_list_append(cmd_list, rsc->active);
            }
            if (rsc->recurring_ops) {
                cmd_list = g_list_concat(cmd_list, rsc->recurring_ops);
            }
            if (rsc->pending_ops) {
                cmd_list = g_list_concat(cmd_list, rsc->pending_ops);
            }
            rsc->pending_ops = rsc->recurring_ops = NULL;
        }
    }

    if (!cmd_list) {
        return;
    }

    crm_err("Connection to fencer failed, finalizing %d pending operations",
            g_list_length(cmd_list));
    for (cmd_iter = cmd_list; cmd_iter; cmd_iter = cmd_iter->next) {
        stonith_action_complete(cmd_iter->data, -ENOTCONN);
    }
    g_list_free(cmd_list);
}

/*!
 * \internal
 * \brief Execute a stonith resource "start" action
 *
 * Start a stonith resource by registering it with the fencer.
 * (Stonith agents don't have a start command.)
 *
 * \param[in] stonith_api  Connection to fencer
 * \param[in] rsc          Stonith resource to start
 * \param[in] cmd          Start command to execute
 *
 * \return pcmk_ok on success, -errno otherwise
 */
static int
execd_stonith_start(stonith_t *stonith_api, lrmd_rsc_t *rsc, lrmd_cmd_t *cmd)
{
    char *key = NULL;
    char *value = NULL;
    stonith_key_value_t *device_params = NULL;
    int rc = pcmk_ok;

    // Convert command parameters to stonith API key/values
    if (cmd->params) {
        GHashTableIter iter;

        g_hash_table_iter_init(&iter, cmd->params);
        while (g_hash_table_iter_next(&iter, (gpointer *) & key, (gpointer *) & value)) {
            device_params = stonith_key_value_add(device_params, key, value);
        }
    }

    /* The fencer will automatically register devices via CIB notifications
     * when the CIB changes, but to avoid a possible race condition between
     * the fencer receiving the notification and the executor requesting that
     * resource, the executor registers the device as well. The fencer knows how
     * to handle duplicate registrations.
     */
    rc = stonith_api->cmds->register_device(stonith_api, st_opt_sync_call,
                                            cmd->rsc_id, rsc->provider,
                                            rsc->type, device_params);

    stonith_key_value_freeall(device_params, 1, 1);
    return rc;
}

/*!
 * \internal
 * \brief Execute a stonith resource "stop" action
 *
 * Stop a stonith resource by unregistering it with the fencer.
 * (Stonith agents don't have a stop command.)
 *
 * \param[in] stonith_api  Connection to fencer
 * \param[in] rsc          Stonith resource to stop
 *
 * \return pcmk_ok on success, -errno otherwise
 */
static inline int
execd_stonith_stop(stonith_t *stonith_api, const lrmd_rsc_t *rsc)
{
    /* @TODO Failure would indicate a problem communicating with fencer;
     * perhaps we should try reconnecting and retrying a few times?
     */
    return stonith_api->cmds->remove_device(stonith_api, st_opt_sync_call,
                                            rsc->rsc_id);
}

/*!
 * \internal
 * \brief Initiate a stonith resource agent recurring "monitor" action
 *
 * \param[in] stonith_api  Connection to fencer
 * \param[in] rsc          Stonith resource to monitor
 * \param[in] cmd          Monitor command being executed
 *
 * \return pcmk_ok if monitor was successfully initiated, -errno otherwise
 */
static inline int
execd_stonith_monitor(stonith_t *stonith_api, lrmd_rsc_t *rsc, lrmd_cmd_t *cmd)
{
    int rc = stonith_api->cmds->monitor(stonith_api, 0, cmd->rsc_id,
                                        cmd->timeout / 1000);

    rc = stonith_api->cmds->register_callback(stonith_api, rc, 0, 0, cmd,
                                              "lrmd_stonith_callback",
                                              lrmd_stonith_callback);
    if (rc == TRUE) {
        rsc->active = cmd;
        rc = pcmk_ok;
    } else {
        rc = -pcmk_err_generic;
    }
    return rc;
}

static void
lrmd_rsc_execute_stonith(lrmd_rsc_t * rsc, lrmd_cmd_t * cmd)
{
    int rc = 0;
    bool do_monitor = FALSE;

    stonith_t *stonith_api = get_stonith_connection();

    if (!stonith_api) {
        rc = -ENOTCONN;

    } else if (safe_str_eq(cmd->action, "start")) {
        rc = execd_stonith_start(stonith_api, rsc, cmd);
        if (rc == 0) {
            do_monitor = TRUE;
        }

    } else if (safe_str_eq(cmd->action, "stop")) {
        rc = execd_stonith_stop(stonith_api, rsc);

    } else if (safe_str_eq(cmd->action, "monitor")) {
        if (cmd->interval_ms > 0) {
            do_monitor = TRUE;
        } else {
            rc = rsc->st_probe_rc;
        }
    }

    if (do_monitor) {
        rc = execd_stonith_monitor(stonith_api, rsc, cmd);
        if (rc == pcmk_ok) {
            // Don't clean up yet, we will find out result of the monitor later
            return;
        }
    }

    stonith_action_complete(cmd, rc);
}

static int
lrmd_rsc_execute_service_lib(lrmd_rsc_t * rsc, lrmd_cmd_t * cmd)
{
    svc_action_t *action = NULL;
    GHashTable *params_copy = NULL;

    CRM_ASSERT(rsc);
    CRM_ASSERT(cmd);

    crm_trace("Creating action, resource:%s action:%s class:%s provider:%s agent:%s",
              rsc->rsc_id, cmd->action, rsc->class, rsc->provider, rsc->type);

#if SUPPORT_NAGIOS
    /* Recurring operations are cancelled anyway for a stop operation */
    if (safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_NAGIOS)
        && safe_str_eq(cmd->action, "stop")) {

        cmd->exec_rc = PCMK_OCF_OK;
        goto exec_done;
    }
#endif

    params_copy = crm_str_table_dup(cmd->params);

    action = resources_action_create(rsc->rsc_id, rsc->class, rsc->provider,
                                     rsc->type,
                                     normalize_action_name(rsc, cmd->action),
                                     cmd->interval_ms, cmd->timeout,
                                     params_copy, cmd->service_flags);

    if (!action) {
        crm_err("Failed to create action, action:%s on resource %s", cmd->action, rsc->rsc_id);
        cmd->lrmd_op_status = PCMK_LRM_OP_ERROR;
        goto exec_done;
    }

    action->cb_data = cmd;

    /* 'cmd' may not be valid after this point if
     * services_action_async() returned TRUE
     *
     * Upstart and systemd both synchronously determine monitor/status
     * results and call action_complete (which may free 'cmd') if necessary.
     */
    if (services_action_async(action, action_complete)) {
        return TRUE;
    }

    cmd->exec_rc = action->rc;
    if(action->status != PCMK_LRM_OP_DONE) {
        cmd->lrmd_op_status = action->status;
    } else {
        cmd->lrmd_op_status = PCMK_LRM_OP_ERROR;
    }
    services_action_free(action);
    action = NULL;

  exec_done:
    cmd_finalize(cmd, rsc);
    return TRUE;
}

static gboolean
lrmd_rsc_execute(lrmd_rsc_t * rsc)
{
    lrmd_cmd_t *cmd = NULL;

    CRM_CHECK(rsc != NULL, return FALSE);

    if (rsc->active) {
        crm_trace("%s is still active", rsc->rsc_id);
        return TRUE;
    }

    if (rsc->pending_ops) {
        GList *first = rsc->pending_ops;

        cmd = first->data;
        if (cmd->delay_id) {
            crm_trace
                ("Command %s %s was asked to run too early, waiting for start_delay timeout of %dms",
                 cmd->rsc_id, cmd->action, cmd->start_delay);
            return TRUE;
        }
        rsc->pending_ops = g_list_remove_link(rsc->pending_ops, first);
        g_list_free_1(first);

#ifdef PCMK__TIME_USE_CGT
        if (cmd->t_first_run.tv_sec == 0) {
            clock_gettime(CLOCK_MONOTONIC, &cmd->t_first_run);
        }
        clock_gettime(CLOCK_MONOTONIC, &cmd->t_run);
#elif defined(HAVE_SYS_TIMEB_H)
        if (cmd->t_first_run.time == 0) {
            ftime(&cmd->t_first_run);
        }
        ftime(&cmd->t_run);
#endif
    }

    if (!cmd) {
        crm_trace("Nothing further to do for %s", rsc->rsc_id);
        return TRUE;
    }

    rsc->active = cmd;          /* only one op at a time for a rsc */
    if (cmd->interval_ms) {
        rsc->recurring_ops = g_list_append(rsc->recurring_ops, cmd);
    }

    log_execute(cmd);

    if (safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_STONITH)) {
        lrmd_rsc_execute_stonith(rsc, cmd);
    } else {
        lrmd_rsc_execute_service_lib(rsc, cmd);
    }

    return TRUE;
}

static gboolean
lrmd_rsc_dispatch(gpointer user_data)
{
    return lrmd_rsc_execute(user_data);
}

void
free_rsc(gpointer data)
{
    GListPtr gIter = NULL;
    lrmd_rsc_t *rsc = data;
    int is_stonith = safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_STONITH);

    gIter = rsc->pending_ops;
    while (gIter != NULL) {
        GListPtr next = gIter->next;
        lrmd_cmd_t *cmd = gIter->data;

        /* command was never executed */
        cmd->lrmd_op_status = PCMK_LRM_OP_CANCELLED;
        cmd_finalize(cmd, NULL);

        gIter = next;
    }
    /* frees list, but not list elements. */
    g_list_free(rsc->pending_ops);

    gIter = rsc->recurring_ops;
    while (gIter != NULL) {
        GListPtr next = gIter->next;
        lrmd_cmd_t *cmd = gIter->data;

        if (is_stonith) {
            cmd->lrmd_op_status = PCMK_LRM_OP_CANCELLED;
            /* If a stonith command is in-flight, just mark it as cancelled;
             * it is not safe to finalize/free the cmd until the stonith api
             * says it has either completed or timed out.
             */
            if (rsc->active != cmd) {
                cmd_finalize(cmd, NULL);
            }
        } else {
            /* This command is already handed off to service library,
             * let service library cancel it and tell us via the callback
             * when it is cancelled. The rsc can be safely destroyed
             * even if we are waiting for the cancel result */
            services_action_cancel(rsc->rsc_id,
                                   normalize_action_name(rsc, cmd->action),
                                   cmd->interval_ms);
        }

        gIter = next;
    }
    /* frees list, but not list elements. */
    g_list_free(rsc->recurring_ops);

    free(rsc->rsc_id);
    free(rsc->class);
    free(rsc->provider);
    free(rsc->type);
    mainloop_destroy_trigger(rsc->work);

    free(rsc);
}

static xmlNode *
process_lrmd_signon(crm_client_t *client, xmlNode *request, int call_id)
{
    xmlNode *reply = NULL;
    int rc = pcmk_ok;
    const char *is_ipc_provider = crm_element_value(request, F_LRMD_IS_IPC_PROVIDER);
    const char *protocol_version = crm_element_value(request, F_LRMD_PROTOCOL_VERSION);

    if (compare_version(protocol_version, LRMD_MIN_PROTOCOL_VERSION) < 0) {
        crm_err("Cluster API version must be greater than or equal to %s, not %s",
                LRMD_MIN_PROTOCOL_VERSION, protocol_version);
        rc = -EPROTO;
    }

    reply = create_lrmd_reply(__FUNCTION__, rc, call_id);
    crm_xml_add(reply, F_LRMD_OPERATION, CRM_OP_REGISTER);
    crm_xml_add(reply, F_LRMD_CLIENTID, client->id);
    crm_xml_add(reply, F_LRMD_PROTOCOL_VERSION, LRMD_PROTOCOL_VERSION);

    if (crm_is_true(is_ipc_provider)) {
        // This is a remote connection from a cluster node's controller
#ifdef SUPPORT_REMOTE
        ipc_proxy_add_provider(client);
#endif
    }
    return reply;
}

static int
process_lrmd_rsc_register(crm_client_t * client, uint32_t id, xmlNode * request)
{
    int rc = pcmk_ok;
    lrmd_rsc_t *rsc = build_rsc_from_xml(request);
    lrmd_rsc_t *dup = g_hash_table_lookup(rsc_list, rsc->rsc_id);

    if (dup &&
        safe_str_eq(rsc->class, dup->class) &&
        safe_str_eq(rsc->provider, dup->provider) && safe_str_eq(rsc->type, dup->type)) {

        crm_notice("Ignoring duplicate registration of '%s'", rsc->rsc_id);
        free_rsc(rsc);
        return rc;
    }

    g_hash_table_replace(rsc_list, rsc->rsc_id, rsc);
    crm_info("Cached agent information for '%s'", rsc->rsc_id);
    return rc;
}

static xmlNode *
process_lrmd_get_rsc_info(xmlNode *request, int call_id)
{
    int rc = pcmk_ok;
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, request, LOG_ERR);
    const char *rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);
    xmlNode *reply = NULL;
    lrmd_rsc_t *rsc = NULL;

    if (rsc_id == NULL) {
        rc = -ENODEV;
    } else {
        rsc = g_hash_table_lookup(rsc_list, rsc_id);
        if (rsc == NULL) {
            crm_info("Agent information for '%s' not in cache", rsc_id);
            rc = -ENODEV;
        }
    }

    reply = create_lrmd_reply(__FUNCTION__, rc, call_id);
    if (rsc) {
        crm_xml_add(reply, F_LRMD_RSC_ID, rsc->rsc_id);
        crm_xml_add(reply, F_LRMD_CLASS, rsc->class);
        crm_xml_add(reply, F_LRMD_PROVIDER, rsc->provider);
        crm_xml_add(reply, F_LRMD_TYPE, rsc->type);
    }
    return reply;
}

static int
process_lrmd_rsc_unregister(crm_client_t * client, uint32_t id, xmlNode * request)
{
    int rc = pcmk_ok;
    lrmd_rsc_t *rsc = NULL;
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, request, LOG_ERR);
    const char *rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);

    if (!rsc_id) {
        return -ENODEV;
    }

    rsc = g_hash_table_lookup(rsc_list, rsc_id);
    if (rsc == NULL) {
        crm_info("Ignoring unregistration of resource '%s', which is not registered",
                 rsc_id);
        return pcmk_ok;
    }

    if (rsc->active) {
        /* let the caller know there are still active ops on this rsc to watch for */
        crm_trace("Operation (0x%p) still in progress for unregistered resource %s",
                  rsc->active, rsc_id);
        rc = -EINPROGRESS;
    }

    g_hash_table_remove(rsc_list, rsc_id);

    return rc;
}

static int
process_lrmd_rsc_exec(crm_client_t * client, uint32_t id, xmlNode * request)
{
    lrmd_rsc_t *rsc = NULL;
    lrmd_cmd_t *cmd = NULL;
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, request, LOG_ERR);
    const char *rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);
    int call_id;

    if (!rsc_id) {
        return -EINVAL;
    }
    if (!(rsc = g_hash_table_lookup(rsc_list, rsc_id))) {
        crm_info("Resource '%s' not found (%d active resources)",
                 rsc_id, g_hash_table_size(rsc_list));
        return -ENODEV;
    }

    cmd = create_lrmd_cmd(request, client);
    call_id = cmd->call_id;

    /* Don't reference cmd after handing it off to be scheduled.
     * The cmd could get merged and freed. */
    schedule_lrmd_cmd(rsc, cmd);

    return call_id;
}

static int
cancel_op(const char *rsc_id, const char *action, guint interval_ms)
{
    GListPtr gIter = NULL;
    lrmd_rsc_t *rsc = g_hash_table_lookup(rsc_list, rsc_id);

    /* How to cancel an action.
     * 1. Check pending ops list, if it hasn't been handed off
     *    to the service library or stonith recurring list remove
     *    it there and that will stop it.
     * 2. If it isn't in the pending ops list, then it's either a
     *    recurring op in the stonith recurring list, or the service
     *    library's recurring list.  Stop it there
     * 3. If not found in any lists, then this operation has either
     *    been executed already and is not a recurring operation, or
     *    never existed.
     */
    if (!rsc) {
        return -ENODEV;
    }

    for (gIter = rsc->pending_ops; gIter != NULL; gIter = gIter->next) {
        lrmd_cmd_t *cmd = gIter->data;

        if (safe_str_eq(cmd->action, action)
            && (cmd->interval_ms == interval_ms)) {

            cmd->lrmd_op_status = PCMK_LRM_OP_CANCELLED;
            cmd_finalize(cmd, rsc);
            return pcmk_ok;
        }
    }

    if (safe_str_eq(rsc->class, PCMK_RESOURCE_CLASS_STONITH)) {
        /* The service library does not handle stonith operations.
         * We have to handle recurring stonith operations ourselves. */
        for (gIter = rsc->recurring_ops; gIter != NULL; gIter = gIter->next) {
            lrmd_cmd_t *cmd = gIter->data;

            if (safe_str_eq(cmd->action, action)
                && (cmd->interval_ms == interval_ms)) {

                cmd->lrmd_op_status = PCMK_LRM_OP_CANCELLED;
                if (rsc->active != cmd) {
                    cmd_finalize(cmd, rsc);
                }
                return pcmk_ok;
            }
        }
    } else if (services_action_cancel(rsc_id,
                                      normalize_action_name(rsc, action),
                                      interval_ms) == TRUE) {
        /* The service library will tell the action_complete callback function
         * this action was cancelled, which will destroy the cmd and remove
         * it from the recurring_op list. Do not do that in this function
         * if the service library says it cancelled it. */
        return pcmk_ok;
    }

    return -EOPNOTSUPP;
}

static void
cancel_all_recurring(lrmd_rsc_t * rsc, const char *client_id)
{
    GList *cmd_list = NULL;
    GList *cmd_iter = NULL;

    /* Notice a copy of each list is created when concat is called.
     * This prevents odd behavior from occurring when the cmd_list
     * is iterated through later on.  It is possible the cancel_op
     * function may end up modifying the recurring_ops and pending_ops
     * lists.  If we did not copy those lists, our cmd_list iteration
     * could get messed up.*/
    if (rsc->recurring_ops) {
        cmd_list = g_list_concat(cmd_list, g_list_copy(rsc->recurring_ops));
    }
    if (rsc->pending_ops) {
        cmd_list = g_list_concat(cmd_list, g_list_copy(rsc->pending_ops));
    }
    if (!cmd_list) {
        return;
    }

    for (cmd_iter = cmd_list; cmd_iter; cmd_iter = cmd_iter->next) {
        lrmd_cmd_t *cmd = cmd_iter->data;

        if (cmd->interval_ms == 0) {
            continue;
        }

        if (client_id && safe_str_neq(cmd->client_id, client_id)) {
            continue;
        }

        cancel_op(rsc->rsc_id, cmd->action, cmd->interval_ms);
    }
    /* frees only the copied list data, not the cmds */
    g_list_free(cmd_list);
}

static int
process_lrmd_rsc_cancel(crm_client_t * client, uint32_t id, xmlNode * request)
{
    xmlNode *rsc_xml = get_xpath_object("//" F_LRMD_RSC, request, LOG_ERR);
    const char *rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);
    const char *action = crm_element_value(rsc_xml, F_LRMD_RSC_ACTION);
    guint interval_ms = 0;

    crm_element_value_ms(rsc_xml, F_LRMD_RSC_INTERVAL, &interval_ms);

    if (!rsc_id || !action) {
        return -EINVAL;
    }

    return cancel_op(rsc_id, action, interval_ms);
}

static void
add_recurring_op_xml(xmlNode *reply, lrmd_rsc_t *rsc)
{
    xmlNode *rsc_xml = create_xml_node(reply, F_LRMD_RSC);

    crm_xml_add(rsc_xml, F_LRMD_RSC_ID, rsc->rsc_id);
    for (GList *item = rsc->recurring_ops; item != NULL; item = item->next) {
        lrmd_cmd_t *cmd = item->data;
        xmlNode *op_xml = create_xml_node(rsc_xml, T_LRMD_RSC_OP);

        crm_xml_add(op_xml, F_LRMD_RSC_ACTION,
                    (cmd->real_action? cmd->real_action : cmd->action));
        crm_xml_add_ms(op_xml, F_LRMD_RSC_INTERVAL, cmd->interval_ms);
        crm_xml_add_int(op_xml, F_LRMD_TIMEOUT, cmd->timeout_orig);
    }
}

static xmlNode *
process_lrmd_get_recurring(xmlNode *request, int call_id)
{
    int rc = pcmk_ok;
    const char *rsc_id = NULL;
    lrmd_rsc_t *rsc = NULL;
    xmlNode *reply = NULL;
    xmlNode *rsc_xml = NULL;

    // Resource ID is optional
    rsc_xml = first_named_child(request, F_LRMD_CALLDATA);
    if (rsc_xml) {
        rsc_xml = first_named_child(rsc_xml, F_LRMD_RSC);
    }
    if (rsc_xml) {
        rsc_id = crm_element_value(rsc_xml, F_LRMD_RSC_ID);
    }

    // If resource ID is specified, resource must exist
    if (rsc_id != NULL) {
        rsc = g_hash_table_lookup(rsc_list, rsc_id);
        if (rsc == NULL) {
            crm_info("Resource '%s' not found (%d active resources)",
                     rsc_id, g_hash_table_size(rsc_list));
            rc = -ENODEV;
        }
    }

    reply = create_lrmd_reply(__FUNCTION__, rc, call_id);

    // If resource ID is not specified, check all resources
    if (rsc_id == NULL) {
        GHashTableIter iter;
        char *key = NULL;

        g_hash_table_iter_init(&iter, rsc_list);
        while (g_hash_table_iter_next(&iter, (gpointer *) &key,
                                      (gpointer *) &rsc)) {
            add_recurring_op_xml(reply, rsc);
        }
    } else if (rsc) {
        add_recurring_op_xml(reply, rsc);
    }
    return reply;
}

void
process_lrmd_message(crm_client_t * client, uint32_t id, xmlNode * request)
{
    int rc = pcmk_ok;
    int call_id = 0;
    const char *op = crm_element_value(request, F_LRMD_OPERATION);
    int do_reply = 0;
    int do_notify = 0;
    xmlNode *reply = NULL;

    crm_trace("Processing %s operation from %s", op, client->id);
    crm_element_value_int(request, F_LRMD_CALLID, &call_id);

    if (crm_str_eq(op, CRM_OP_IPC_FWD, TRUE)) {
#ifdef SUPPORT_REMOTE
        ipc_proxy_forward_client(client, request);
#endif
        do_reply = 1;
    } else if (crm_str_eq(op, CRM_OP_REGISTER, TRUE)) {
        reply = process_lrmd_signon(client, request, call_id);
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_RSC_REG, TRUE)) {
        rc = process_lrmd_rsc_register(client, id, request);
        do_notify = 1;
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_RSC_INFO, TRUE)) {
        reply = process_lrmd_get_rsc_info(request, call_id);
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_RSC_UNREG, TRUE)) {
        rc = process_lrmd_rsc_unregister(client, id, request);
        /* don't notify anyone about failed un-registers */
        if (rc == pcmk_ok || rc == -EINPROGRESS) {
            do_notify = 1;
        }
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_RSC_EXEC, TRUE)) {
        rc = process_lrmd_rsc_exec(client, id, request);
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_RSC_CANCEL, TRUE)) {
        rc = process_lrmd_rsc_cancel(client, id, request);
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_POKE, TRUE)) {
        do_notify = 1;
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_CHECK, TRUE)) {
        xmlNode *data = get_message_xml(request, F_LRMD_CALLDATA); 
        const char *timeout = crm_element_value(data, F_LRMD_WATCHDOG);
        CRM_LOG_ASSERT(data != NULL);
        crm_trace("Checking sbd-timeout %s.", timeout);
        check_sbd_timeout(timeout);
    } else if (crm_str_eq(op, LRMD_OP_ALERT_EXEC, TRUE)) {
        rc = process_lrmd_alert_exec(client, id, request);
        do_reply = 1;
    } else if (crm_str_eq(op, LRMD_OP_GET_RECURRING, TRUE)) {
        reply = process_lrmd_get_recurring(request, call_id);
        do_reply = 1;
    } else {
        rc = -EOPNOTSUPP;
        do_reply = 1;
        crm_err("Unknown %s from %s", op, client->name);
        crm_log_xml_warn(request, "UnknownOp");
    }

    crm_debug("Processed %s operation from %s: rc=%d, reply=%d, notify=%d",
              op, client->id, rc, do_reply, do_notify);

    if (do_reply) {
        int send_rc = pcmk_ok;

        if (reply == NULL) {
            reply = create_lrmd_reply(__FUNCTION__, rc, call_id);
        }
        send_rc = lrmd_server_send_reply(client, id, reply);
        free_xml(reply);
        if (send_rc < 0) {
            crm_warn("Reply to client %s failed: %s " CRM_XS " %d",
                     client->name, pcmk_strerror(send_rc), send_rc);
        }
    }

    if (do_notify) {
        send_generic_notify(rc, request);
    }
}
