/*
 * Copyright 2012-2026 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU General Public License version 2
 * or later (GPLv2+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>

#include <errno.h>
#include <stdbool.h>

#include <crm/crm.h>
#include <crm/common/xml.h>
#include <crm/lrmd_internal.h>          // lrmd__*

#include <pacemaker-internal.h>
#include <pacemaker-controld.h>

static GHashTable *lrm_state_table = NULL;

/*!
 * \internal
 * \brief Free an \c lrmd_rsc_info_t object
 *
 * This is a wrapper for \c lrmd_free_rsc_info for use with items in a
 * \c GHashTable.
 *
 * \param[in,out] data  Resource info to free (<tt>lrmd_rsc_info_t *</tt>)
 *
 * \note This is a \c GDestroyNotify.
 */
static void
free_rsc_info(void *data)
{
    lrmd_rsc_info_t *rsc_info = data;

    lrmd_free_rsc_info(rsc_info);
}

/*!
 * \internal
 * \brief Free a pending deletion operation
 *
 * \param[in,out] data  Operation to free
 *                      (<tt>struct pending_deletion_op_s *</tt>)
 *
 * \note This is a \c GDestroyNotify.
 */
static void
free_pending_deletion_op(void *data)
{
    struct pending_deletion_op_s *op = data;

    free(op->rsc);
    delete_ha_msg_input(op->input);
    free(op);
}

/*!
 * \internal
 * \brief Free a recurring operation
 *
 * \param[in,out] data  Operation to free (<tt>active_op_t *</tt>)
 *
 * \note This is a \c GDestroyNotify.
 */
static void
free_recurring_op(void *data)
{
    active_op_t *op = data;

    free(op->rsc_id);
    free(op->op_type);
    free(op->op_key);
    free(op->transition_key);
    g_clear_pointer(&op->params, g_hash_table_destroy);
    free(op);
}

/*!
 * \internal
 * \brief Fail a pending operation in response to executor disconnection
 *
 * \param[in]     key        Executor call key (<tt>const char *</tt>)
 * \param[in,out] value      Operation (<tt>active_op_t *</tt>)
 * \param[in,out] user_data  Executor state (<tt>lrm_state_t *</tt>)
 *
 * \return \c true (to remove \p key and \p value from the hash table)
 *
 * \note This is a \c GHRFunc.
 */
static gboolean
fail_pending_op(void *key, void *value, void *user_data)
{
    const char *call_key = key;
    active_op_t *op = value;
    lrm_state_t *lrm_state = user_data;

    lrmd_event_data_t *event = lrmd_new_event(op->rsc_id, op->op_type,
                                              op->interval_ms);

    pcmk__trace("Preemptively failing " PCMK__OP_FMT " on %s (call=%s, %s)",
                op->rsc_id, op->op_type, op->interval_ms,
                lrm_state->node_name, call_key, op->transition_key);

    event->type = lrmd_event_exec_complete;
    event->user_data = pcmk__str_copy(op->transition_key);
    event->call_id = op->call_id;
    event->t_run = op->start_time;
    event->t_rcchange = op->start_time;
    event->params = pcmk__str_table_dup(op->params);
    event->remote_nodename = pcmk__str_copy(lrm_state->node_name);

    lrmd__set_result(event, PCMK_OCF_UNKNOWN_ERROR, PCMK_EXEC_NOT_CONNECTED,
                     "Action was pending when executor connection was dropped");

    process_lrm_event(lrm_state, event, op, NULL);
    lrmd_free_event(event);
    return true;
}

/*!
 * \internal
 * \brief Create executor state entry for a node and add it to the state table
 *
 * \param[in]  node_name  Node to create entry for
 *
 * \return Newly allocated executor state object initialized for \p node_name
 */
static lrm_state_t *
lrm_state_create(const char *node_name)
{
    lrm_state_t *state = NULL;

    if (!node_name) {
        pcmk__err("No node name given for lrm state object");
        return NULL;
    }

    state = pcmk__assert_alloc(1, sizeof(lrm_state_t));

    state->node_name = pcmk__str_copy(node_name);
    state->rsc_info_cache = pcmk__strkey_table(NULL, free_rsc_info);
    state->deletion_ops = pcmk__strkey_table(free, free_pending_deletion_op);
    state->active_ops = pcmk__strkey_table(free, free_recurring_op);
    state->resource_history = pcmk__strkey_table(NULL, history_free);
    state->metadata_cache = metadata_cache_new();

    g_hash_table_insert(lrm_state_table, (char *)state->node_name, state);
    return state;
}

static void
internal_lrm_state_destroy(void *data)
{
    lrm_state_t *lrm_state = data;

    if (!lrm_state) {
        return;
    }

    /* Rather than directly remove the recorded proxy entries from proxy_table,
     * make sure any connected proxies get disconnected. So that
     * remote_proxy_disconnected() will be called and as well remove the
     * entries from proxy_table.
     */
    controld_remote_proxy_disconnect_node(lrm_state->node_name);

    remote_ra_cleanup(lrm_state);
    lrmd_api_delete(lrm_state->conn);

    g_clear_pointer(&lrm_state->rsc_info_cache, g_hash_table_destroy);
    g_clear_pointer(&lrm_state->resource_history, g_hash_table_destroy);
    g_clear_pointer(&lrm_state->deletion_ops, g_hash_table_destroy);
    g_clear_pointer(&lrm_state->active_ops, g_hash_table_destroy);

    metadata_cache_free(lrm_state->metadata_cache);

    free((char *)lrm_state->node_name);
    free(lrm_state);
}

void
lrm_state_reset_tables(lrm_state_t *lrm_state)
{
    pcmk__trace("Resetting resource history cache with %u members",
                g_hash_table_size(lrm_state->resource_history));
    g_hash_table_remove_all(lrm_state->resource_history);

    pcmk__trace("Resetting deletion operations cache with %u members",
                g_hash_table_size(lrm_state->deletion_ops));
    g_hash_table_remove_all(lrm_state->deletion_ops);

    pcmk__trace("Resetting active operations cache with %u members",
                g_hash_table_size(lrm_state->active_ops));
    g_hash_table_remove_all(lrm_state->active_ops);

    pcmk__trace("Resetting resource information cache with %u members",
                g_hash_table_size(lrm_state->rsc_info_cache));
    g_hash_table_remove_all(lrm_state->rsc_info_cache);
}

void
controld_execd_state_table_init(void)
{
    if (lrm_state_table != NULL) {
        return;
    }

    lrm_state_table = pcmk__strikey_table(NULL, internal_lrm_state_destroy);
}

void
controld_execd_state_table_free(void)
{
    g_clear_pointer(&lrm_state_table, g_hash_table_destroy);
}

/*!
 * \internal
 * \brief Get executor state object
 *
 * \param[in] node_name  Get executor state for this node (local node if NULL)
 * \param[in] create     If true, create executor state if it doesn't exist
 *
 * \return Executor state object for \p node_name
 */
lrm_state_t *
controld_get_executor_state(const char *node_name, bool create)
{
    lrm_state_t *state = NULL;

    if ((node_name == NULL) && (controld_globals.cluster != NULL)) {
        node_name = controld_globals.cluster->priv->node_name;
    }
    if ((node_name == NULL) || (lrm_state_table == NULL)) {
        return NULL;
    }

    state = g_hash_table_lookup(lrm_state_table, node_name);
    if ((state == NULL) && create) {
        state = lrm_state_create(node_name);
    }
    return state;
}

/* @TODO the lone caller just needs to iterate over the values, so replace this
 * with a g_hash_table_foreach() wrapper instead
 */
GList *
lrm_state_get_list(void)
{
    if (lrm_state_table == NULL) {
        return NULL;
    }
    return g_hash_table_get_values(lrm_state_table);
}

/*!
 * \internal
 * \brief Cancel a given operation if it's recurring
 *
 * \param[in]     key        Executor call key (<tt>const char *</tt>)
 * \param[in]     value      Operation (<tt>const active_op_t *</tt>)
 * \param[in,out] user_data  Executor state (<tt>lrm_state_t *</tt>)
 *
 * \return \c true if a cancellation request was sent to the executor
 *         successfully, or \c false otherwise (including if the operation was
 *         not found or was already canceled)
 *
 * \note This is a \c GHRFunc.
 */
static gboolean
cancel_recurring_op(void *key, void *value, void *user_data)
{
    const char *call_key = key;
    const active_op_t *op = value;
    lrm_state_t *lrm_state = user_data;

    if (op->interval_ms == 0) {
        return false;
    }

    pcmk__info("Cancelling op %d for %s (%s)", op->call_id, op->rsc_id,
               call_key);

    return !controld_execd_cancel_op(lrm_state, op->rsc_id, call_key,
                                     op->call_id, false);
}

/*!
 * \internal
 * \brief Check whether a resource should be logged as active on a given node
 *
 * This function is for logging purposes only. It's called when the controller
 * is exiting or disconnecting from the executor, to determine whether to log a
 * message noting that the resource is active.
 *
 * We check the resource history of the node to which \p lrm_state belongs.
 *
 * A resource is considered inactive on the node if its last recorded operation
 * there:
 * * returned \c PCMK_OCF_NOT_RUNNING
 * * returned \c PCMK_OCF_NOT_CONFIGURED and was not a recurring operation
 * * returned \c PCMK_OCF_OK and was a \c PCMK_ACTION_STOP or
 *   \c PCMK_ACTION_MIGRATE_TO operation
 *
 * Otherwise, the resource is considered active.
 *
 * \param[in] lrm_state  Executor state
 * \param[in] rsc_id     Resource ID
 *
 * \return \c true if the resource should be logged as active, or \c false
 *         otherwise
 */
static bool
is_rsc_active(const lrm_state_t *lrm_state, const char *rsc_id)
{
    const rsc_history_t *entry = NULL;
    const lrmd_event_data_t *last = NULL;

    entry = g_hash_table_lookup(lrm_state->resource_history, rsc_id);
    if ((entry == NULL) || (entry->last == NULL)) {
        return false;
    }

    last = entry->last;

    pcmk__trace("Processing %s: %s.%d=%d", rsc_id, last->op_type,
                last->interval_ms, last->rc);

    if (last->rc == PCMK_OCF_NOT_RUNNING) {
        return false;
    }

    if ((last->interval_ms == 0) && (last->rc == PCMK_OCF_NOT_CONFIGURED)) {
        /* The resource probably never started due to misconfiguration. Don't
         * let the caller log the resource as active.
         */
        return false;
    }

    if (last->rc != PCMK_OCF_OK) {
        // Resource may be active, so let the caller log it as active
        return true;
    }

    if (pcmk__str_eq(last->op_type, PCMK_ACTION_STOP, pcmk__str_none)) {
        // Resource has cleanly stopped
        return false;
    }

    if (pcmk__str_eq(last->op_type, PCMK_ACTION_MIGRATE_TO, pcmk__str_none)) {
        // Resource has successfully migrated to another node
        return false;
    }

    // Last operation was successful and left the resource active
    return true;
}

/*!
 * \internal
 * \brief Increment a counter if a given operation is non-recurring
 *
 * \param[in]     key        Ignored
 * \param[in]     value      Operation (<tt>const active_op_t *</tt>)
 * \param[in,out] user_data  Counter (<tt>unsigned int *</tt>)
 *
 * \note This is a \c GHFunc.
 */
static void
count_non_recurring_op(void *key, void *value, void *user_data)
{
    const active_op_t *op = value;
    unsigned int *count = user_data;

    if (op->interval_ms == 0) {
        (*count)++;
    }
}

/*!
 * \internal
 * \brief Log a given pending operation at a given level
 *
 * \param[in] key        Executor call key (<tt>const char *</tt>)
 * \param[in] value      Operation (<tt>const active_op_t *</tt>)
 * \param[in] user_data  Log level (<tt>GINT_TO_POINTER(<int>)</tt>)
 *
 * \note This is a \c GHFunc.
 */
static void
log_pending_op(void *key, void *value, void *user_data)
{
    const char *call_key = key;
    const active_op_t *op = value;
    int log_level = GPOINTER_TO_INT(user_data);

    do_crm_log(log_level, "Pending operation: %s (%s)", call_key, op->op_key);
}

/*!
 * \internal
 * \brief User data for \c log_incomplete_op()
 */
struct log_incomplete_op_data {
    //! Resource ID to match
    const char *id;

    //! Event that triggered the function call (for logging only)
    const char *when;
};

/*!
 * \internal
 * \brief Log a given incomplete operation if it matches a given resource ID
 *
 * The operation is logged only if its \c rsc_id field matches \p user_data->id.
 *
 * \param[in] key        Executor call key (<tt>const char *</tt>)
 * \param[in] value      Operation (<tt>const active_op_t *</tt>)
 * \param[in] user_data  User data
 *                       (<tt>const struct log_incomplete_op_data *</tt>)
 *
 * \note This is a \c GHFunc.
 */
static void
log_incomplete_op(void *key, void *value, void *user_data)
{
    const char *call_key = key;
    const active_op_t *op = value;
    const struct log_incomplete_op_data *data = user_data;

    if (!pcmk__str_eq(data->id, op->rsc_id, pcmk__str_none)) {
        return;
    }

    pcmk__notice("Recurring action %s (%s) incomplete at %s", call_key,
                 op->op_key, data->when);
}

bool
lrm_state_verify_stopped(lrm_state_t *lrm_state, enum crmd_fsa_state cur_state,
                         int log_level)
{
    unsigned int count = 0;
    const char *when = "lrm disconnect";

    GHashTableIter iter;
    const rsc_history_t *entry = NULL;

    pcmk__assert(lrm_state != NULL);

    pcmk__debug("Checking for active resources before exit");

    if (cur_state == S_TERMINATE) {
        log_level = LOG_ERR;
        when = "shutdown";

    } else if (pcmk__is_set(controld_globals.fsa_input_register, R_SHUTDOWN)) {
        when = "shutdown... waiting";
    }

    if (g_hash_table_size(lrm_state->active_ops) > 0) {
        unsigned int size = g_hash_table_size(lrm_state->active_ops);
        unsigned int removed = 0;

        if (lrm_state->conn->cmds->is_connected(lrm_state->conn)) {
            removed = g_hash_table_foreach_remove(lrm_state->active_ops,
                                                  cancel_recurring_op,
                                                  lrm_state);
            size -= removed;
        }

        pcmk__notice("Canceled %u recurring operation%s at %s (%u operations "
                     "remaining)", removed, pcmk__plural_s(removed), when,
                     size);

        /* Ignore recurring operations. Don't just subtract removed from the
         * original size, because lrm_state->conn may not be connected, or
         * cancel_recurring_op() may return false for a recurring operation.
         */
        g_hash_table_foreach(lrm_state->active_ops, count_non_recurring_op,
                             &count);
    }

    if (count > 0) {
        do_crm_log(log_level, "%u pending executor operation%s at %s", count,
                   pcmk__plural_s(count), when);

        if ((cur_state != S_TERMINATE)
            && pcmk__is_set(controld_globals.fsa_input_register,
                            R_SENT_RSC_STOP)) {

            return false;
        }

        g_hash_table_foreach(lrm_state->active_ops, log_pending_op,
                             GINT_TO_POINTER(log_level));
        return true;
    }

    // There are no non-recurring actions in lrm_state->active_ops

    if (pcmk__is_set(controld_globals.fsa_input_register, R_SHUTDOWN)) {
        /* At this point we're not waiting, we're just shutting down */
        when = "shutdown";
    }

    count = 0;
    g_hash_table_iter_init(&iter, lrm_state->resource_history);
    while (g_hash_table_iter_next(&iter, NULL, (void **) &entry)) {
        const struct log_incomplete_op_data data = {
            .id = entry->id,
            .when = when,
        };

        if (!is_rsc_active(lrm_state, entry->id)) {
            continue;
        }

        count++;
        if (log_level == LOG_ERR) {
            pcmk__info("Found %s active at %s", entry->id, when);

        } else {
            pcmk__trace("Found %s active at %s", entry->id, when);
        }

        g_hash_table_foreach(lrm_state->active_ops, log_incomplete_op,
                             (void *) &data);
    }

    if (count > 0) {
        pcmk__err("%u resource%s active at %s", count,
                  pcmk__plural_alt(count, " was", "s were"), when);
    }

    return true;
}

void
controld_execd_state_disconnect(lrm_state_t *lrm_state)
{
    unsigned int removed = 0;

    if (!lrm_state->conn) {
        return;
    }
    pcmk__trace("Disconnecting %s", lrm_state->node_name);

    controld_remote_proxy_disconnect_node(lrm_state->node_name);

    lrm_state->conn->cmds->disconnect(lrm_state->conn);

    if (!pcmk__is_set(controld_globals.fsa_input_register, R_SHUTDOWN)) {
        removed = g_hash_table_foreach_remove(lrm_state->active_ops,
                                              fail_pending_op, lrm_state);
        pcmk__trace("Synthesized %u operation failures for %s", removed,
                    lrm_state->node_name);
    }
}

// \return Standard Pacemaker return code
int
controld_connect_local_executor(lrm_state_t *lrm_state)
{
    int rc = pcmk_rc_ok;

    if (lrm_state->conn == NULL) {
        lrm_state->conn = lrmd_api_new();
        lrm_state->conn->cmds->set_callback(lrm_state->conn, lrm_op_callback);
    }

    rc = lrm_state->conn->cmds->connect(lrm_state->conn, CRM_SYSTEM_CRMD, NULL);
    rc = pcmk_legacy2rc(rc);

    if (rc == pcmk_rc_ok) {
        lrm_state->num_lrm_register_fails = 0;
    } else {
        lrm_state->num_lrm_register_fails++;
    }
    return rc;
}

// \return Standard Pacemaker return code
int
controld_connect_remote_executor(lrm_state_t *lrm_state, const char *server,
                                 int port, int timeout_ms)
{
    int rc = pcmk_rc_ok;

    if (lrm_state->conn == NULL) {
        lrm_state->conn = lrmd_remote_api_new(lrm_state->node_name, server,
                                              port);
        lrm_state->conn->cmds->set_callback(lrm_state->conn,
                                            remote_lrm_op_callback);
        lrmd__proxy_set_callback(lrm_state->conn, lrm_state,
                                 controld_remote_proxy_cb);
    }

    pcmk__trace("Initiating remote connection to %s:%d with timeout %dms",
                server, port, timeout_ms);
    rc = lrm_state->conn->cmds->connect_async(lrm_state->conn,
                                              lrm_state->node_name, timeout_ms);
    if (rc == pcmk_ok) {
        lrm_state->num_lrm_register_fails = 0;
    } else {
        lrm_state->num_lrm_register_fails++; // Ignored for remote connections
    }
    return pcmk_legacy2rc(rc);
}

int
lrm_state_get_metadata(lrm_state_t * lrm_state,
                       const char *class,
                       const char *provider,
                       const char *agent, char **output, enum lrmd_call_options options)
{
    lrmd_key_value_t *params = NULL;

    if (!lrm_state->conn) {
        return -ENOTCONN;
    }

    /* Add the node name to the environment, as is done with normal resource
     * action calls. Meta-data calls shouldn't need it, but some agents are
     * written with an ocf_local_nodename call at the beginning regardless of
     * action. Without the environment variable, the agent would try to contact
     * the controller to get the node name -- but the controller would be
     * blocking on the synchronous meta-data call.
     *
     * At this point, we have to assume that agents are unlikely to make other
     * calls that require the controller, such as crm_node --quorum or
     * --cluster-id.
     *
     * @TODO Make meta-data calls asynchronous. (This will be part of a larger
     * project to make meta-data calls via the executor rather than directly.)
     */
    params = lrmd_key_value_add(params, CRM_META "_" PCMK__META_ON_NODE,
                                lrm_state->node_name);

    return lrm_state->conn->cmds->get_metadata_params(lrm_state->conn, class,
                                                      provider, agent, output,
                                                      options, params);
}

int
lrm_state_cancel(lrm_state_t *lrm_state, const char *rsc_id, const char *action,
                 unsigned int interval_ms)
{
    if (!lrm_state->conn) {
        return -ENOTCONN;
    }

    /* Figure out a way to make this async?
     * NOTICE: Currently it's synced and directly acknowledged in
     * controld_invoke_execd().
     */
    if (is_remote_lrmd_ra(NULL, NULL, rsc_id)) {
        return remote_ra_cancel(lrm_state, rsc_id, action, interval_ms);
    }
    return lrm_state->conn->cmds->cancel(lrm_state->conn, rsc_id, action,
                                         interval_ms);
}

lrmd_rsc_info_t *
lrm_state_get_rsc_info(lrm_state_t * lrm_state, const char *rsc_id, enum lrmd_call_options options)
{
    lrmd_rsc_info_t *rsc = NULL;

    if (!lrm_state->conn) {
        return NULL;
    }
    if (is_remote_lrmd_ra(NULL, NULL, rsc_id)) {
        return remote_ra_get_rsc_info(lrm_state, rsc_id);
    }

    rsc = g_hash_table_lookup(lrm_state->rsc_info_cache, rsc_id);
    if (rsc == NULL) {
        /* only contact the lrmd if we don't already have a cached rsc info */
        rsc = lrm_state->conn->cmds->get_rsc_info(lrm_state->conn, rsc_id,
                                                  options);
        if (rsc == NULL) {
		    return NULL;
        }
        /* cache the result */
        g_hash_table_insert(lrm_state->rsc_info_cache, rsc->id, rsc);
    }

    return lrmd_copy_rsc_info(rsc);

}

/*!
 * \internal
 * \brief Initiate a resource agent action
 *
 * \param[in,out] lrm_state       Executor state object
 * \param[in]     rsc_id          ID of resource for action
 * \param[in]     action          Action to execute
 * \param[in]     userdata        String to copy and pass to execution callback
 * \param[in]     interval_ms     Action interval (in milliseconds)
 * \param[in]     timeout_ms      Action timeout (in milliseconds)
 * \param[in]     start_delay_ms  Delay (in ms) before initiating action
 * \param[in]     parameters      Hash table of resource parameters
 * \param[out]    call_id         Where to store call ID on success
 *
 * \return Standard Pacemaker return code
 */
int
controld_execute_resource_agent(lrm_state_t *lrm_state, const char *rsc_id,
                                const char *action, const char *userdata,
                                unsigned int interval_ms, int timeout_ms,
                                int start_delay_ms, GHashTable *parameters,
                                int *call_id)
{
    int rc = pcmk_rc_ok;
    lrmd_key_value_t *params = NULL;

    if (lrm_state->conn == NULL) {
        return ENOTCONN;
    }

    // Convert parameters from hash table to list
    if (parameters != NULL) {
        const char *key = NULL;
        const char *value = NULL;
        GHashTableIter iter;

        g_hash_table_iter_init(&iter, parameters);
        while (g_hash_table_iter_next(&iter, (void **) &key,
                                      (void **) &value)) {
            params = lrmd_key_value_add(params, key, value);
        }
    }

    if (is_remote_lrmd_ra(NULL, NULL, rsc_id)) {
        rc = controld_execute_remote_agent(lrm_state, rsc_id, action,
                                           userdata, interval_ms, timeout_ms,
                                           start_delay_ms, params, call_id);

    } else {
        rc = lrm_state->conn->cmds->exec(lrm_state->conn, rsc_id, action,
                                         userdata, interval_ms, timeout_ms,
                                         start_delay_ms,
                                         lrmd_opt_notify_changes_only, params);
        if (rc < 0) {
            rc = pcmk_legacy2rc(rc);
        } else {
            *call_id = rc;
            rc = pcmk_rc_ok;
        }
    }
    return rc;
}

int
lrm_state_register_rsc(lrm_state_t *lrm_state, const char *rsc_id,
                       const char *class, const char *provider,
                       const char *agent, enum lrmd_call_options options)
{
    if (lrm_state->conn == NULL) {
        return -ENOTCONN;
    }

    if (is_remote_lrmd_ra(agent, provider, NULL)) {
        return controld_get_executor_state(rsc_id, true)? pcmk_ok : -EINVAL;
    }

    /* @TODO Implement an asynchronous version of this (currently a blocking
     * call to the lrmd).
     */
    return lrm_state->conn->cmds->register_rsc(lrm_state->conn, rsc_id, class,
                                               provider, agent, options);
}

int
lrm_state_unregister_rsc(lrm_state_t *lrm_state, const char *rsc_id,
                         enum lrmd_call_options options)
{
    if (lrm_state->conn == NULL) {
        return -ENOTCONN;
    }

    if (is_remote_lrmd_ra(NULL, NULL, rsc_id)) {
        g_hash_table_remove(lrm_state_table, rsc_id);
        return pcmk_ok;
    }

    g_hash_table_remove(lrm_state->rsc_info_cache, rsc_id);

    /* @TODO Optimize this ... this function is a blocking round trip from
     * client to daemon. The controld_execd_state.c code path that uses this
     * function should always treat it as an async operation. The executor API
     * should make an async version available.
     */
    return lrm_state->conn->cmds->unregister_rsc(lrm_state->conn, rsc_id,
                                                 options);
}
