/*
 * Copyright 2004-2019 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <ctype.h>
#include <libgen.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <glib.h>

#include <crm/crm.h>
#include <crm/stonith-ng.h>
#include <crm/fencing/internal.h>
#include <crm/msg_xml.h>
#include <crm/common/xml.h>

#include <crm/common/mainloop.h>

#if SUPPORT_CIBSECRETS
#  include <crm/common/cib_secrets.h>
#endif

CRM_TRACE_INIT_DATA(stonith);

struct stonith_action_s {
    /*! user defined data */
    char *agent;
    char *action;
    char *victim;
    GHashTable *args;
    int timeout;
    int async;
    void *userdata;
    void (*done_cb) (GPid pid, gint status, const char *output, gpointer user_data);
    void (*fork_cb) (GPid pid, gpointer user_data);

    svc_action_t *svc_action;

    /*! internal timing information */
    time_t initial_start_time;
    int tries;
    int remaining_timeout;
    int max_retries;

    /* device output data */
    GPid pid;
    int rc;
    char *output;
    char *error;
};

typedef struct stonith_private_s {
    char *token;
    crm_ipc_t *ipc;
    mainloop_io_t *source;
    GHashTable *stonith_op_callback_table;
    GList *notify_list;
    int notify_refcnt;
    bool notify_deletes;

    void (*op_callback) (stonith_t * st, stonith_callback_data_t * data);

} stonith_private_t;

typedef struct stonith_notify_client_s {
    const char *event;
    const char *obj_id;         /* implement one day */
    const char *obj_type;       /* implement one day */
    void (*notify) (stonith_t * st, stonith_event_t * e);
    bool delete;

} stonith_notify_client_t;

typedef struct stonith_callback_client_s {
    void (*callback) (stonith_t * st, stonith_callback_data_t * data);
    const char *id;
    void *user_data;
    gboolean only_success;
    gboolean allow_timeout_updates;
    struct timer_rec_s *timer;

} stonith_callback_client_t;

struct notify_blob_s {
    stonith_t *stonith;
    xmlNode *xml;
};

struct timer_rec_s {
    int call_id;
    int timeout;
    guint ref;
    stonith_t *stonith;
};

typedef int (*stonith_op_t) (const char *, int, const char *, xmlNode *,
                             xmlNode *, xmlNode *, xmlNode **, xmlNode **);

bool stonith_dispatch(stonith_t * st);
xmlNode *stonith_create_op(int call_id, const char *token, const char *op, xmlNode * data,
                           int call_options);
static int stonith_send_command(stonith_t *stonith, const char *op,
                                xmlNode *data, xmlNode **output_data,
                                int call_options, int timeout);

static void stonith_connection_destroy(gpointer user_data);
static void stonith_send_notification(gpointer data, gpointer user_data);
static int internal_stonith_action_execute(stonith_action_t * action);
static void log_action(stonith_action_t *action, pid_t pid);

/*!
 * \brief Get agent namespace by name
 *
 * \param[in] namespace_s  Name of namespace as string
 *
 * \return Namespace as enum value
 */
enum stonith_namespace
stonith_text2namespace(const char *namespace_s)
{
    if ((namespace_s == NULL) || !strcmp(namespace_s, "any")) {
        return st_namespace_any;

    } else if (!strcmp(namespace_s, "redhat")
               || !strcmp(namespace_s, "stonith-ng")) {
        return st_namespace_rhcs;

    } else if (!strcmp(namespace_s, "heartbeat")) {
        return st_namespace_lha;
    }
    return st_namespace_invalid;
}

/*!
 * \brief Get agent namespace name
 *
 * \param[in] namespace  Namespace as enum value
 *
 * \return Namespace name as string
 */
const char *
stonith_namespace2text(enum stonith_namespace st_namespace)
{
    switch (st_namespace) {
        case st_namespace_any:      return "any";
        case st_namespace_rhcs:     return "stonith-ng";
        case st_namespace_lha:      return "heartbeat";
        default:                    break;
    }
    return "unsupported";
}

/*!
 * \brief Determine namespace of a fence agent
 *
 * \param[in] agent        Fence agent type
 * \param[in] namespace_s  Name of agent namespace as string, if known
 *
 * \return Namespace of specified agent, as enum value
 */
enum stonith_namespace
stonith_get_namespace(const char *agent, const char *namespace_s)
{
    if (stonith__agent_is_rhcs(agent)) {
        return st_namespace_rhcs;
    }

#if HAVE_STONITH_STONITH_H
    if (stonith__agent_is_lha(agent)) {
        return st_namespace_lha;
    }
#endif

    crm_err("Unknown fence agent: %s", agent);
    return st_namespace_invalid;
}

static void
log_action(stonith_action_t *action, pid_t pid)
{
    if (action->output) {
        /* Logging the whole string confuses syslog when the string is xml */
        char *prefix = crm_strdup_printf("%s[%d] stdout:", action->agent, pid);

        crm_log_output(LOG_TRACE, prefix, action->output);
        free(prefix);
    }

    if (action->error) {
        /* Logging the whole string confuses syslog when the string is xml */
        char *prefix = crm_strdup_printf("%s[%d] stderr:", action->agent, pid);

        crm_log_output(LOG_WARNING, prefix, action->error);
        free(prefix);
    }
}

/* when cycling through the list we don't want to delete items
   so just mark them and when we know nobody is using the list
   loop over it to remove the marked items
 */
static void
foreach_notify_entry (stonith_private_t *private,
                GFunc func,
                gpointer user_data)
{
    private->notify_refcnt++;
    g_list_foreach(private->notify_list, func, user_data);
    private->notify_refcnt--;
    if ((private->notify_refcnt == 0) &&
        private->notify_deletes) {
        GList *list_item = private->notify_list;

        private->notify_deletes = FALSE;
        while (list_item != NULL)
        {
            stonith_notify_client_t *list_client = list_item->data;
            GList *next = g_list_next(list_item);

            if (list_client->delete) {
                free(list_client);
                private->notify_list =
                    g_list_delete_link(private->notify_list, list_item);
            }
            list_item = next;
        }
    }
}

static void
stonith_connection_destroy(gpointer user_data)
{
    stonith_t *stonith = user_data;
    stonith_private_t *native = NULL;
    struct notify_blob_s blob;

    crm_trace("Sending destroyed notification");
    blob.stonith = stonith;
    blob.xml = create_xml_node(NULL, "notify");

    native = stonith->st_private;
    native->ipc = NULL;
    native->source = NULL;

    free(native->token); native->token = NULL;
    stonith->state = stonith_disconnected;
    crm_xml_add(blob.xml, F_TYPE, T_STONITH_NOTIFY);
    crm_xml_add(blob.xml, F_SUBTYPE, T_STONITH_NOTIFY_DISCONNECT);

    foreach_notify_entry(native, stonith_send_notification, &blob);
    free_xml(blob.xml);
}

xmlNode *
create_device_registration_xml(const char *id, enum stonith_namespace namespace,
                               const char *agent, stonith_key_value_t *params,
                               const char *rsc_provides)
{
    xmlNode *data = create_xml_node(NULL, F_STONITH_DEVICE);
    xmlNode *args = create_xml_node(data, XML_TAG_ATTRS);

#if HAVE_STONITH_STONITH_H
    if (namespace == st_namespace_any) {
        namespace = stonith_get_namespace(agent, NULL);
    }
    if (namespace == st_namespace_lha) {
        hash2field((gpointer) "plugin", (gpointer) agent, args);
        agent = "fence_legacy";
    }
#endif

    crm_xml_add(data, XML_ATTR_ID, id);
    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);
    crm_xml_add(data, "agent", agent);
    if ((namespace != st_namespace_any) && (namespace != st_namespace_invalid)) {
        crm_xml_add(data, "namespace", stonith_namespace2text(namespace));
    }
    if (rsc_provides) {
        crm_xml_add(data, "rsc_provides", rsc_provides);
    }

    for (; params; params = params->next) {
        hash2field((gpointer) params->key, (gpointer) params->value, args);
    }

    return data;
}

static int
stonith_api_register_device(stonith_t * st, int call_options,
                            const char *id, const char *namespace, const char *agent,
                            stonith_key_value_t * params)
{
    int rc = 0;
    xmlNode *data = NULL;

    data = create_device_registration_xml(id, stonith_text2namespace(namespace),
                                          agent, params, NULL);

    rc = stonith_send_command(st, STONITH_OP_DEVICE_ADD, data, NULL, call_options, 0);
    free_xml(data);

    return rc;
}

static int
stonith_api_remove_device(stonith_t * st, int call_options, const char *name)
{
    int rc = 0;
    xmlNode *data = NULL;

    data = create_xml_node(NULL, F_STONITH_DEVICE);
    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);
    crm_xml_add(data, XML_ATTR_ID, name);
    rc = stonith_send_command(st, STONITH_OP_DEVICE_DEL, data, NULL, call_options, 0);
    free_xml(data);

    return rc;
}

static int
stonith_api_remove_level_full(stonith_t *st, int options,
                              const char *node, const char *pattern,
                              const char *attr, const char *value, int level)
{
    int rc = 0;
    xmlNode *data = NULL;

    CRM_CHECK(node || pattern || (attr && value), return -EINVAL);

    data = create_xml_node(NULL, XML_TAG_FENCING_LEVEL);
    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);

    if (node) {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET, node);

    } else if (pattern) {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_PATTERN, pattern);

    } else {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_ATTRIBUTE, attr);
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_VALUE, value);
    }

    crm_xml_add_int(data, XML_ATTR_STONITH_INDEX, level);
    rc = stonith_send_command(st, STONITH_OP_LEVEL_DEL, data, NULL, options, 0);
    free_xml(data);

    return rc;
}

static int
stonith_api_remove_level(stonith_t * st, int options, const char *node, int level)
{
    return stonith_api_remove_level_full(st, options, node,
                                         NULL, NULL, NULL, level);
}

/*!
 * \internal
 * \brief Create XML for fence topology level registration request
 *
 * \param[in] node        If not NULL, target level by this node name
 * \param[in] pattern     If not NULL, target by node name using this regex
 * \param[in] attr        If not NULL, target by this node attribute
 * \param[in] value       If not NULL, target by this node attribute value
 * \param[in] level       Index number of level to register
 * \param[in] device_list List of devices in level
 *
 * \return Newly allocated XML tree on success, NULL otherwise
 *
 * \note The caller should set only one of node, pattern or attr/value.
 */
xmlNode *
create_level_registration_xml(const char *node, const char *pattern,
                              const char *attr, const char *value,
                              int level, stonith_key_value_t *device_list)
{
    int len = 0;
    char *list = NULL;
    xmlNode *data;

    CRM_CHECK(node || pattern || (attr && value), return NULL);

    data = create_xml_node(NULL, XML_TAG_FENCING_LEVEL);
    CRM_CHECK(data, return NULL);

    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);
    crm_xml_add_int(data, XML_ATTR_ID, level);
    crm_xml_add_int(data, XML_ATTR_STONITH_INDEX, level);

    if (node) {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET, node);

    } else if (pattern) {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_PATTERN, pattern);

    } else {
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_ATTRIBUTE, attr);
        crm_xml_add(data, XML_ATTR_STONITH_TARGET_VALUE, value);
    }

    for (; device_list; device_list = device_list->next) {

        int adding = strlen(device_list->value);
        if(list) {
            adding++;                                      /* +1 space */
        }

        crm_trace("Adding %s (%dc) at offset %d", device_list->value, adding, len);
        list = realloc_safe(list, len + adding + 1);       /* +1 EOS */
        if (list == NULL) {
            crm_perror(LOG_CRIT, "Could not create device list");
            free_xml(data);
            return NULL;
        }
        sprintf(list + len, "%s%s", len?",":"", device_list->value);
        len += adding;
    }

    crm_xml_add(data, XML_ATTR_STONITH_DEVICES, list);

    free(list);
    return data;
}

static int
stonith_api_register_level_full(stonith_t * st, int options, const char *node,
                                const char *pattern,
                                const char *attr, const char *value,
                                int level, stonith_key_value_t *device_list)
{
    int rc = 0;
    xmlNode *data = create_level_registration_xml(node, pattern, attr, value,
                                                  level, device_list);
    CRM_CHECK(data != NULL, return -EINVAL);

    rc = stonith_send_command(st, STONITH_OP_LEVEL_ADD, data, NULL, options, 0);
    free_xml(data);

    return rc;
}

static int
stonith_api_register_level(stonith_t * st, int options, const char *node, int level,
                           stonith_key_value_t * device_list)
{
    return stonith_api_register_level_full(st, options, node, NULL, NULL, NULL,
                                           level, device_list);
}

static void
append_arg(const char *key, const char *value, GHashTable **args)
{
    CRM_CHECK(key != NULL, return);
    CRM_CHECK(value != NULL, return);
    CRM_CHECK(args != NULL, return);

    if (strstr(key, "pcmk_")) {
        return;
    } else if (strstr(key, CRM_META)) {
        return;
    } else if (safe_str_eq(key, "crm_feature_set")) {
        return;
    }

    if (!*args) {
        *args = crm_str_table_new();
    }

    CRM_CHECK(*args != NULL, return);
    crm_trace("Appending: %s=%s", key, value);
    g_hash_table_replace(*args, strdup(key), strdup(value));
}

static void
append_config_arg(gpointer key, gpointer value, gpointer user_data)
{
    /* The fencer will filter action out when it registers the device,
     * but ignore it here just in case any other library callers
     * fail to do so.
     */
    if (safe_str_neq(key, STONITH_ATTR_ACTION_OP)) {
        append_arg(key, value, user_data);
        return;
    }
}

static GHashTable *
make_args(const char *agent, const char *action, const char *victim, uint32_t victim_nodeid, GHashTable * device_args,
          GHashTable * port_map)
{
    char buffer[512];
    GHashTable *arg_list = NULL;
    const char *value = NULL;

    CRM_CHECK(action != NULL, return NULL);

    snprintf(buffer, sizeof(buffer), "pcmk_%s_action", action);
    if (device_args) {
        value = g_hash_table_lookup(device_args, buffer);
    }
    if (value) {
        crm_debug("Substituting action '%s' for requested operation '%s'", value, action);
        action = value;
    }

    append_arg(STONITH_ATTR_ACTION_OP, action, &arg_list);
    if (victim && device_args) {
        const char *alias = victim;
        const char *param = g_hash_table_lookup(device_args, STONITH_ATTR_HOSTARG);

        if (port_map && g_hash_table_lookup(port_map, victim)) {
            alias = g_hash_table_lookup(port_map, victim);
        }

        /* Always supply the node's name, too:
         * https://github.com/ClusterLabs/fence-agents/blob/master/doc/FenceAgentAPI.md
         */
        append_arg("nodename", victim, &arg_list);
        if (victim_nodeid) {
            char nodeid_str[33] = { 0, };
            if (snprintf(nodeid_str, 33, "%u", (unsigned int)victim_nodeid)) {
                crm_info("For stonith action (%s) for victim %s, adding nodeid (%s) to parameters",
                         action, victim, nodeid_str);
                append_arg("nodeid", nodeid_str, &arg_list);
            }
        }

        /* Check if we need to supply the victim in any other form */
        if(safe_str_eq(agent, "fence_legacy")) {
            value = agent;

        } else if (param == NULL) {
            param = "port";
            value = g_hash_table_lookup(device_args, param);

        } else if (safe_str_eq(param, "none")) {
            value = param;      /* Nothing more to do */

        } else {
            value = g_hash_table_lookup(device_args, param);
        }

        /* Don't overwrite explictly set values for $param */
        if (value == NULL || safe_str_eq(value, "dynamic")) {
            crm_debug("Performing '%s' action targeting '%s' as '%s=%s'", action, victim, param,
                      alias);
            append_arg(param, alias, &arg_list);
        }
    }

    if (device_args) {
        g_hash_table_foreach(device_args, append_config_arg, &arg_list);
    }

    return arg_list;
}

/*!
 * \internal
 * \brief Free all memory used by a stonith action
 *
 * \param[in,out] action  Action to free
 */
void
stonith__destroy_action(stonith_action_t *action)
{
    if (action) {
        free(action->agent);
        if (action->args) {
            g_hash_table_destroy(action->args);
        }
        free(action->action);
        free(action->victim);
        if (action->svc_action) {
            services_action_free(action->svc_action);
        }
        free(action->output);
        free(action->error);
        free(action);
    }
}

/*!
 * \internal
 * \brief Get the result of an executed stonith action
 *
 * \param[in,out] action        Executed action
 * \param[out]    rc            Where to store result code (or NULL)
 * \param[out]    output        Where to store standard output (or NULL)
 * \param[out]    error_output  Where to store standard error output (or NULL)
 *
 * \note If output or error_output is not NULL, the caller is responsible for
 *       freeing the memory.
 */
void
stonith__action_result(stonith_action_t *action, int *rc, char **output,
                       char **error_output)
{
    if (rc) {
        *rc = pcmk_ok;
    }
    if (output) {
        *output = NULL;
    }
    if (error_output) {
        *error_output = NULL;
    }
    if (action != NULL) {
        if (rc) {
            *rc = action->rc;
        }
        if (output && action->output) {
            *output = action->output;
            action->output = NULL; // hand off memory management to caller
        }
        if (error_output && action->error) {
            *error_output = action->error;
            action->error = NULL; // hand off memory management to caller
        }
    }
}

#define FAILURE_MAX_RETRIES 2
stonith_action_t *
stonith_action_create(const char *agent,
                      const char *_action,
                      const char *victim,
                      uint32_t victim_nodeid,
                      int timeout, GHashTable * device_args, GHashTable * port_map)
{
    stonith_action_t *action;

    action = calloc(1, sizeof(stonith_action_t));
    action->args = make_args(agent, _action, victim, victim_nodeid, device_args, port_map);
    crm_debug("Preparing '%s' action for %s using agent %s",
              _action, (victim? victim : "no target"), agent);
    action->agent = strdup(agent);
    action->action = strdup(_action);
    if (victim) {
        action->victim = strdup(victim);
    }
    action->timeout = action->remaining_timeout = timeout;
    action->max_retries = FAILURE_MAX_RETRIES;

    if (device_args) {
        char buffer[512];
        const char *value = NULL;

        snprintf(buffer, sizeof(buffer), "pcmk_%s_retries", _action);
        value = g_hash_table_lookup(device_args, buffer);

        if (value) {
            action->max_retries = atoi(value);
        }
    }

    return action;
}

static gboolean
update_remaining_timeout(stonith_action_t * action)
{
    int diff = time(NULL) - action->initial_start_time;

    if (action->tries >= action->max_retries) {
        crm_info("Attempted to execute agent %s (%s) the maximum number of times (%d) allowed",
                 action->agent, action->action, action->max_retries);
        action->remaining_timeout = 0;
    } else if ((action->rc != -ETIME) && diff < (action->timeout * 0.7)) {
        /* only set remaining timeout period if there is 30%
         * or greater of the original timeout period left */
        action->remaining_timeout = action->timeout - diff;
    } else {
        action->remaining_timeout = 0;
    }
    return action->remaining_timeout ? TRUE : FALSE;
}

static int
svc_action_to_errno(svc_action_t *svc_action) {
    int rv = pcmk_ok;

    if (svc_action->rc > 0) {
        /* Try to provide a useful error code based on the fence agent's
            * error output.
            */
        if (svc_action->rc == PCMK_OCF_TIMEOUT) {
            rv = -ETIME;

        } else if (svc_action->stderr_data == NULL) {
            rv = -ENODATA;

        } else if (strstr(svc_action->stderr_data, "imed out")) {
            /* Some agents have their own internal timeouts */
            rv = -ETIME;

        } else if (strstr(svc_action->stderr_data, "Unrecognised action")) {
            rv = -EOPNOTSUPP;

        } else {
            rv = -pcmk_err_generic;
        }
    }
    return rv;
}

static void
stonith_action_async_done(svc_action_t *svc_action)
{
    stonith_action_t *action = (stonith_action_t *) svc_action->cb_data;

    action->rc = svc_action_to_errno(svc_action);
    action->output = svc_action->stdout_data;
    svc_action->stdout_data = NULL;
    action->error = svc_action->stderr_data;
    svc_action->stderr_data = NULL;

    svc_action->params = NULL;

    crm_debug("Child process %d performing action '%s' exited with rc %d",
                action->pid, action->action, svc_action->rc);

    log_action(action, action->pid);

    if (action->rc != pcmk_ok && update_remaining_timeout(action)) {
        int rc = internal_stonith_action_execute(action);
        if (rc == pcmk_ok) {
            return;
        }
    }

    if (action->done_cb) {
        action->done_cb(action->pid, action->rc, action->output, action->userdata);
    }

    action->svc_action = NULL; // don't remove our caller
    stonith__destroy_action(action);
}

static void
stonith_action_async_forked(svc_action_t *svc_action)
{
    stonith_action_t *action = (stonith_action_t *) svc_action->cb_data;

    action->pid = svc_action->pid;
    action->svc_action = svc_action;

    if (action->fork_cb) {
        (action->fork_cb) (svc_action->pid, action->userdata);
    }

    crm_trace("Child process %d performing action '%s' successfully forked",
              action->pid, action->action);
}

static int
internal_stonith_action_execute(stonith_action_t * action)
{
    int rc = -EPROTO;
    int is_retry = 0;
    svc_action_t *svc_action = NULL;
    static int stonith_sequence = 0;
    char *buffer = NULL;

    if (!action->tries) {
        action->initial_start_time = time(NULL);
    }
    action->tries++;

    if (action->tries > 1) {
        crm_info("Attempt %d to execute %s (%s). remaining timeout is %d",
                 action->tries, action->agent, action->action, action->remaining_timeout);
        is_retry = 1;
    }

    if (action->args == NULL || action->agent == NULL)
        goto fail;

    buffer = crm_strdup_printf(RH_STONITH_DIR "/%s", basename(action->agent));
    svc_action = services_action_create_generic(buffer, NULL);
    free(buffer);
    svc_action->timeout = 1000 * action->remaining_timeout;
    svc_action->standard = strdup(PCMK_RESOURCE_CLASS_STONITH);
    svc_action->id = crm_strdup_printf("%s_%s_%d", basename(action->agent),
                                       action->action, action->tries);
    svc_action->agent = strdup(action->agent);
    svc_action->sequence = stonith_sequence++;
    svc_action->params = action->args;
    svc_action->cb_data = (void *) action;
    set_bit(svc_action->flags, SVC_ACTION_NON_BLOCKED);

    /* keep retries from executing out of control and free previous results */
    if (is_retry) {
        free(action->output);
        action->output = NULL;
        free(action->error);
        action->error = NULL;
        sleep(1);
    }

    if (action->async) {
        /* async */
        if(services_action_async_fork_notify(svc_action,
            &stonith_action_async_done,
            &stonith_action_async_forked) == FALSE) {
            services_action_free(svc_action);
            svc_action = NULL;
        } else {
            rc = 0;
        }

    } else {
        /* sync */
        if (services_action_sync(svc_action)) {
            rc = 0;
            action->rc = svc_action_to_errno(svc_action);
            action->output = svc_action->stdout_data;
            svc_action->stdout_data = NULL;
            action->error = svc_action->stderr_data;
            svc_action->stderr_data = NULL;
        } else {
            action->rc = -ECONNABORTED;
            rc = action->rc;
        }

        svc_action->params = NULL;
        services_action_free(svc_action);
    }

  fail:
    return rc;
}

/*!
 * \internal
 * \brief Kick off execution of an async stonith action
 *
 * \param[in,out] action        Action to be executed
 * \param[in,out] userdata      Datapointer to be passed to callbacks
 * \param[in]     done          Callback to notify action has failed/succeeded
 * \param[in]     fork_callback Callback to notify successful fork of child
 *
 * \return pcmk_ok if ownership of action has been taken, -errno otherwise
 */
int
stonith_action_execute_async(stonith_action_t * action,
                             void *userdata,
                             void (*done) (GPid pid, int rc, const char *output,
                                           gpointer user_data),
                             void (*fork_cb) (GPid pid, gpointer user_data))
{
    if (!action) {
        return -EINVAL;
    }

    action->userdata = userdata;
    action->done_cb = done;
    action->fork_cb = fork_cb;
    action->async = 1;

    return internal_stonith_action_execute(action);
}

/*!
 * \internal
 * \brief Execute a stonith action
 *
 * \param[in,out] action  Action to execute
 *
 * \return pcmk_ok on success, -errno otherwise
 */
int
stonith__execute(stonith_action_t *action)
{
    int rc = pcmk_ok;

    CRM_CHECK(action != NULL, return -EINVAL);

    // Keep trying until success, max retries, or timeout
    do {
        rc = internal_stonith_action_execute(action);
    } while ((rc != pcmk_ok) && update_remaining_timeout(action));

    return rc;
}

static int
stonith_api_device_list(stonith_t * stonith, int call_options, const char *namespace,
                        stonith_key_value_t ** devices, int timeout)
{
    int count = 0;
    enum stonith_namespace ns = stonith_text2namespace(namespace);

    if (devices == NULL) {
        crm_err("Parameter error: stonith_api_device_list");
        return -EFAULT;
    }

#if HAVE_STONITH_STONITH_H
    // Include Linux-HA agents if requested
    if ((ns == st_namespace_any) || (ns == st_namespace_lha)) {
        count += stonith__list_lha_agents(devices);
    }
#endif

    // Include Red Hat agents if requested
    if ((ns == st_namespace_any) || (ns == st_namespace_rhcs)) {
        count += stonith__list_rhcs_agents(devices);
    }

    return count;
}

static int
stonith_api_device_metadata(stonith_t * stonith, int call_options, const char *agent,
                            const char *namespace, char **output, int timeout)
{
    /* By executing meta-data directly, we can get it from stonith_admin when
     * the cluster is not running, which is important for higher-level tools.
     */

    enum stonith_namespace ns = stonith_get_namespace(agent, namespace);

    crm_trace("Looking up metadata for %s agent %s",
              stonith_namespace2text(ns), agent);

    switch (ns) {
        case st_namespace_rhcs:
            return stonith__rhcs_metadata(agent, timeout, output);

#if HAVE_STONITH_STONITH_H
        case st_namespace_lha:
            return stonith__lha_metadata(agent, timeout, output);
#endif

        default:
            crm_err("Can't get fence agent '%s' meta-data: No such agent",
                    agent);
            break;
    }
    return -ENODEV;
}

static int
stonith_api_query(stonith_t * stonith, int call_options, const char *target,
                  stonith_key_value_t ** devices, int timeout)
{
    int rc = 0, lpc = 0, max = 0;

    xmlNode *data = NULL;
    xmlNode *output = NULL;
    xmlXPathObjectPtr xpathObj = NULL;

    CRM_CHECK(devices != NULL, return -EINVAL);

    data = create_xml_node(NULL, F_STONITH_DEVICE);
    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);
    crm_xml_add(data, F_STONITH_TARGET, target);
    crm_xml_add(data, F_STONITH_ACTION, "off");
    rc = stonith_send_command(stonith, STONITH_OP_QUERY, data, &output, call_options, timeout);

    if (rc < 0) {
        return rc;
    }

    xpathObj = xpath_search(output, "//@agent");
    if (xpathObj) {
        max = numXpathResults(xpathObj);

        for (lpc = 0; lpc < max; lpc++) {
            xmlNode *match = getXpathResult(xpathObj, lpc);

            CRM_LOG_ASSERT(match != NULL);
            if(match != NULL) {
                xmlChar *match_path = xmlGetNodePath(match);

                crm_info("%s[%d] = %s", "//@agent", lpc, match_path);
                free(match_path);
                *devices = stonith_key_value_add(*devices, NULL, crm_element_value(match, XML_ATTR_ID));
            }
        }

        freeXpathObject(xpathObj);
    }

    free_xml(output);
    free_xml(data);
    return max;
}

static int
stonith_api_call(stonith_t * stonith,
                 int call_options,
                 const char *id,
                 const char *action, const char *victim, int timeout, xmlNode ** output)
{
    int rc = 0;
    xmlNode *data = NULL;

    data = create_xml_node(NULL, F_STONITH_DEVICE);
    crm_xml_add(data, F_STONITH_ORIGIN, __FUNCTION__);
    crm_xml_add(data, F_STONITH_DEVICE, id);
    crm_xml_add(data, F_STONITH_ACTION, action);
    crm_xml_add(data, F_STONITH_TARGET, victim);

    rc = stonith_send_command(stonith, STONITH_OP_EXEC, data, output, call_options, timeout);
    free_xml(data);

    return rc;
}

static int
stonith_api_list(stonith_t * stonith, int call_options, const char *id, char **list_info,
                 int timeout)
{
    int rc;
    xmlNode *output = NULL;

    rc = stonith_api_call(stonith, call_options, id, "list", NULL, timeout, &output);

    if (output && list_info) {
        const char *list_str;

        list_str = crm_element_value(output, "st_output");

        if (list_str) {
            *list_info = strdup(list_str);
        }
    }

    if (output) {
        free_xml(output);
    }

    return rc;
}

static int
stonith_api_monitor(stonith_t * stonith, int call_options, const char *id, int timeout)
{
    return stonith_api_call(stonith, call_options, id, "monitor", NULL, timeout, NULL);
}

static int
stonith_api_status(stonith_t * stonith, int call_options, const char *id, const char *port,
                   int timeout)
{
    return stonith_api_call(stonith, call_options, id, "status", port, timeout, NULL);
}

static int
stonith_api_fence(stonith_t * stonith, int call_options, const char *node, const char *action,
                  int timeout, int tolerance)
{
    int rc = 0;
    xmlNode *data = NULL;

    data = create_xml_node(NULL, __FUNCTION__);
    crm_xml_add(data, F_STONITH_TARGET, node);
    crm_xml_add(data, F_STONITH_ACTION, action);
    crm_xml_add_int(data, F_STONITH_TIMEOUT, timeout);
    crm_xml_add_int(data, F_STONITH_TOLERANCE, tolerance);

    rc = stonith_send_command(stonith, STONITH_OP_FENCE, data, NULL, call_options, timeout);
    free_xml(data);

    return rc;
}

static int
stonith_api_confirm(stonith_t * stonith, int call_options, const char *target)
{
    return stonith_api_fence(stonith, call_options | st_opt_manual_ack, target, "off", 0, 0);
}

static int
stonith_api_history(stonith_t * stonith, int call_options, const char *node,
                    stonith_history_t ** history, int timeout)
{
    int rc = 0;
    xmlNode *data = NULL;
    xmlNode *output = NULL;
    stonith_history_t *last = NULL;

    *history = NULL;

    if (node) {
        data = create_xml_node(NULL, __FUNCTION__);
        crm_xml_add(data, F_STONITH_TARGET, node);
    }

    rc = stonith_send_command(stonith, STONITH_OP_FENCE_HISTORY, data, &output,
                              call_options | st_opt_sync_call, timeout);
    free_xml(data);

    if (rc == 0) {
        xmlNode *op = NULL;
        xmlNode *reply = get_xpath_object("//" F_STONITH_HISTORY_LIST, output,
                                          LOG_NEVER);

        for (op = __xml_first_child(reply); op != NULL; op = __xml_next(op)) {
            stonith_history_t *kvp;
            int completed;

            kvp = calloc(1, sizeof(stonith_history_t));
            kvp->target = crm_element_value_copy(op, F_STONITH_TARGET);
            kvp->action = crm_element_value_copy(op, F_STONITH_ACTION);
            kvp->origin = crm_element_value_copy(op, F_STONITH_ORIGIN);
            kvp->delegate = crm_element_value_copy(op, F_STONITH_DELEGATE);
            kvp->client = crm_element_value_copy(op, F_STONITH_CLIENTNAME);
            crm_element_value_int(op, F_STONITH_DATE, &completed);
            kvp->completed = (time_t) completed;
            crm_element_value_int(op, F_STONITH_STATE, &kvp->state);

            if (last) {
                last->next = kvp;
            } else {
                *history = kvp;
            }
            last = kvp;
        }
    }

    free_xml(output);

    return rc;
}

void stonith_history_free(stonith_history_t *history)
{
    stonith_history_t *hp, *hp_old;

    for (hp = history; hp; hp_old = hp, hp = hp->next, free(hp_old)) {
        free(hp->target);
        free(hp->action);
        free(hp->origin);
        free(hp->delegate);
        free(hp->client);
    }
}

static gint
stonithlib_GCompareFunc(gconstpointer a, gconstpointer b)
{
    int rc = 0;
    const stonith_notify_client_t *a_client = a;
    const stonith_notify_client_t *b_client = b;

    if (a_client->delete || b_client->delete) {
        /* make entries marked for deletion not findable */
        return -1;
    }
    CRM_CHECK(a_client->event != NULL && b_client->event != NULL, return 0);
    rc = strcmp(a_client->event, b_client->event);
    if (rc == 0) {
        if (a_client->notify == NULL || b_client->notify == NULL) {
            return 0;

        } else if (a_client->notify == b_client->notify) {
            return 0;

        } else if (((long)a_client->notify) < ((long)b_client->notify)) {
            crm_err("callbacks for %s are not equal: %p vs. %p",
                    a_client->event, a_client->notify, b_client->notify);
            return -1;
        }
        crm_err("callbacks for %s are not equal: %p vs. %p",
                a_client->event, a_client->notify, b_client->notify);
        return 1;
    }
    return rc;
}

xmlNode *
stonith_create_op(int call_id, const char *token, const char *op, xmlNode * data, int call_options)
{
    xmlNode *op_msg = create_xml_node(NULL, "stonith_command");

    CRM_CHECK(op_msg != NULL, return NULL);
    CRM_CHECK(token != NULL, return NULL);

    crm_xml_add(op_msg, F_XML_TAGNAME, "stonith_command");

    crm_xml_add(op_msg, F_TYPE, T_STONITH_NG);
    crm_xml_add(op_msg, F_STONITH_CALLBACK_TOKEN, token);
    crm_xml_add(op_msg, F_STONITH_OPERATION, op);
    crm_xml_add_int(op_msg, F_STONITH_CALLID, call_id);
    crm_trace("Sending call options: %.8lx, %d", (long)call_options, call_options);
    crm_xml_add_int(op_msg, F_STONITH_CALLOPTS, call_options);

    if (data != NULL) {
        add_message_xml(op_msg, F_STONITH_CALLDATA, data);
    }

    return op_msg;
}

static void
stonith_destroy_op_callback(gpointer data)
{
    stonith_callback_client_t *blob = data;

    if (blob->timer && blob->timer->ref > 0) {
        g_source_remove(blob->timer->ref);
    }
    free(blob->timer);
    free(blob);
}

static int
stonith_api_signoff(stonith_t * stonith)
{
    stonith_private_t *native = stonith->st_private;

    crm_debug("Disconnecting from the fencer");

    if (native->source != NULL) {
        /* Attached to mainloop */
        mainloop_del_ipc_client(native->source);
        native->source = NULL;
        native->ipc = NULL;

    } else if (native->ipc) {
        /* Not attached to mainloop */
        crm_ipc_t *ipc = native->ipc;

        native->ipc = NULL;
        crm_ipc_close(ipc);
        crm_ipc_destroy(ipc);
    }

    free(native->token); native->token = NULL;
    stonith->state = stonith_disconnected;
    return pcmk_ok;
}

static int
stonith_api_del_callback(stonith_t * stonith, int call_id, bool all_callbacks)
{
    stonith_private_t *private = stonith->st_private;

    if (all_callbacks) {
        private->op_callback = NULL;
        g_hash_table_destroy(private->stonith_op_callback_table);
        private->stonith_op_callback_table = g_hash_table_new_full(g_direct_hash, g_direct_equal,
                                                                   NULL,
                                                                   stonith_destroy_op_callback);

    } else if (call_id == 0) {
        private->op_callback = NULL;

    } else {
        g_hash_table_remove(private->stonith_op_callback_table, GINT_TO_POINTER(call_id));
    }
    return pcmk_ok;
}

static void
invoke_callback(stonith_t * st, int call_id, int rc, void *userdata,
                void (*callback) (stonith_t * st, stonith_callback_data_t * data))
{
    stonith_callback_data_t data = { 0, };

    data.call_id = call_id;
    data.rc = rc;
    data.userdata = userdata;

    callback(st, &data);
}

static void
stonith_perform_callback(stonith_t * stonith, xmlNode * msg, int call_id, int rc)
{
    stonith_private_t *private = NULL;
    stonith_callback_client_t *blob = NULL;
    stonith_callback_client_t local_blob;

    CRM_CHECK(stonith != NULL, return);
    CRM_CHECK(stonith->st_private != NULL, return);

    private = stonith->st_private;

    local_blob.id = NULL;
    local_blob.callback = NULL;
    local_blob.user_data = NULL;
    local_blob.only_success = FALSE;

    if (msg != NULL) {
        crm_element_value_int(msg, F_STONITH_RC, &rc);
        crm_element_value_int(msg, F_STONITH_CALLID, &call_id);
    }

    CRM_CHECK(call_id > 0, crm_log_xml_err(msg, "Bad result"));

    blob = g_hash_table_lookup(private->stonith_op_callback_table, GINT_TO_POINTER(call_id));

    if (blob != NULL) {
        local_blob = *blob;
        blob = NULL;

        stonith_api_del_callback(stonith, call_id, FALSE);

    } else {
        crm_trace("No callback found for call %d", call_id);
        local_blob.callback = NULL;
    }

    if (local_blob.callback != NULL && (rc == pcmk_ok || local_blob.only_success == FALSE)) {
        crm_trace("Invoking callback %s for call %d", crm_str(local_blob.id), call_id);
        invoke_callback(stonith, call_id, rc, local_blob.user_data, local_blob.callback);

    } else if (private->op_callback == NULL && rc != pcmk_ok) {
        crm_warn("Fencing command failed: %s", pcmk_strerror(rc));
        crm_log_xml_debug(msg, "Failed fence update");
    }

    if (private->op_callback != NULL) {
        crm_trace("Invoking global callback for call %d", call_id);
        invoke_callback(stonith, call_id, rc, NULL, private->op_callback);
    }
    crm_trace("OP callback activated.");
}

static gboolean
stonith_async_timeout_handler(gpointer data)
{
    struct timer_rec_s *timer = data;

    crm_err("Async call %d timed out after %dms", timer->call_id, timer->timeout);
    stonith_perform_callback(timer->stonith, NULL, timer->call_id, -ETIME);

    /* Always return TRUE, never remove the handler
     * We do that in stonith_del_callback()
     */
    return TRUE;
}

static void
set_callback_timeout(stonith_callback_client_t * callback, stonith_t * stonith, int call_id,
                     int timeout)
{
    struct timer_rec_s *async_timer = callback->timer;

    if (timeout <= 0) {
        return;
    }

    if (!async_timer) {
        async_timer = calloc(1, sizeof(struct timer_rec_s));
        callback->timer = async_timer;
    }

    async_timer->stonith = stonith;
    async_timer->call_id = call_id;
    /* Allow a fair bit of grace to allow the server to tell us of a timeout
     * This is only a fallback
     */
    async_timer->timeout = (timeout + 60) * 1000;
    if (async_timer->ref) {
        g_source_remove(async_timer->ref);
    }
    async_timer->ref =
        g_timeout_add(async_timer->timeout, stonith_async_timeout_handler, async_timer);
}

static void
update_callback_timeout(int call_id, int timeout, stonith_t * st)
{
    stonith_callback_client_t *callback = NULL;
    stonith_private_t *private = st->st_private;

    callback = g_hash_table_lookup(private->stonith_op_callback_table, GINT_TO_POINTER(call_id));
    if (!callback || !callback->allow_timeout_updates) {
        return;
    }

    set_callback_timeout(callback, st, call_id, timeout);
}

static int
stonith_dispatch_internal(const char *buffer, ssize_t length, gpointer userdata)
{
    const char *type = NULL;
    struct notify_blob_s blob;

    stonith_t *st = userdata;
    stonith_private_t *private = NULL;

    CRM_ASSERT(st != NULL);
    private = st->st_private;

    blob.stonith = st;
    blob.xml = string2xml(buffer);
    if (blob.xml == NULL) {
        crm_warn("Received malformed message from fencer: %s", buffer);
        return 0;
    }

    /* do callbacks */
    type = crm_element_value(blob.xml, F_TYPE);
    crm_trace("Activating %s callbacks...", type);

    if (safe_str_eq(type, T_STONITH_NG)) {
        stonith_perform_callback(st, blob.xml, 0, 0);

    } else if (safe_str_eq(type, T_STONITH_NOTIFY)) {
        foreach_notify_entry(private, stonith_send_notification, &blob);
    } else if (safe_str_eq(type, T_STONITH_TIMEOUT_VALUE)) {
        int call_id = 0;
        int timeout = 0;

        crm_element_value_int(blob.xml, F_STONITH_TIMEOUT, &timeout);
        crm_element_value_int(blob.xml, F_STONITH_CALLID, &call_id);

        update_callback_timeout(call_id, timeout, st);
    } else {
        crm_err("Unknown message type: %s", type);
        crm_log_xml_warn(blob.xml, "BadReply");
    }

    free_xml(blob.xml);
    return 1;
}

static int
stonith_api_signon(stonith_t * stonith, const char *name, int *stonith_fd)
{
    int rc = pcmk_ok;
    stonith_private_t *native = NULL;
    const char *display_name = name? name : "client";

    static struct ipc_client_callbacks st_callbacks = {
        .dispatch = stonith_dispatch_internal,
        .destroy = stonith_connection_destroy
    };

    CRM_CHECK(stonith != NULL, return -EINVAL);

    native = stonith->st_private;
    CRM_ASSERT(native != NULL);

    crm_debug("Attempting fencer connection by %s with%s mainloop",
              display_name, (stonith_fd? "out" : ""));

    stonith->state = stonith_connected_command;
    if (stonith_fd) {
        /* No mainloop */
        native->ipc = crm_ipc_new("stonith-ng", 0);

        if (native->ipc && crm_ipc_connect(native->ipc)) {
            *stonith_fd = crm_ipc_get_fd(native->ipc);
        } else if (native->ipc) {
            crm_ipc_close(native->ipc);
            crm_ipc_destroy(native->ipc);
            native->ipc = NULL;
        }

    } else {
        /* With mainloop */
        native->source =
            mainloop_add_ipc_client("stonith-ng", G_PRIORITY_MEDIUM, 0, stonith, &st_callbacks);
        native->ipc = mainloop_get_ipc_client(native->source);
    }

    if (native->ipc == NULL) {
        rc = -ENOTCONN;
    } else {
        xmlNode *reply = NULL;
        xmlNode *hello = create_xml_node(NULL, "stonith_command");

        crm_xml_add(hello, F_TYPE, T_STONITH_NG);
        crm_xml_add(hello, F_STONITH_OPERATION, CRM_OP_REGISTER);
        crm_xml_add(hello, F_STONITH_CLIENTNAME, name);
        rc = crm_ipc_send(native->ipc, hello, crm_ipc_client_response, -1, &reply);

        if (rc < 0) {
            crm_debug("Couldn't register with the fencer: %s "
                      CRM_XS " rc=%d", pcmk_strerror(rc), rc);
            rc = -ECOMM;

        } else if (reply == NULL) {
            crm_debug("Couldn't register with the fencer: no reply");
            rc = -EPROTO;

        } else {
            const char *msg_type = crm_element_value(reply, F_STONITH_OPERATION);

            native->token = crm_element_value_copy(reply, F_STONITH_CLIENTID);
            if (safe_str_neq(msg_type, CRM_OP_REGISTER)) {
                crm_debug("Couldn't register with the fencer: invalid reply type '%s'",
                          (msg_type? msg_type : "(missing)"));
                crm_log_xml_debug(reply, "Invalid fencer reply");
                rc = -EPROTO;

            } else if (native->token == NULL) {
                crm_debug("Couldn't register with the fencer: no token in reply");
                crm_log_xml_debug(reply, "Invalid fencer reply");
                rc = -EPROTO;

            } else {
#if HAVE_MSGFROMIPC_TIMEOUT
                stonith->call_timeout = MAX_IPC_DELAY;
#endif
                crm_debug("Connection to fencer by %s succeeded (registration token: %s)",
                          display_name, native->token);
                rc = pcmk_ok;
            }
        }

        free_xml(reply);
        free_xml(hello);
    }

    if (rc != pcmk_ok) {
        crm_debug("Connection attempt to fencer by %s failed: %s "
                  CRM_XS " rc=%d", display_name, pcmk_strerror(rc), rc);
        stonith->cmds->disconnect(stonith);
    }
    return rc;
}

static int
stonith_set_notification(stonith_t * stonith, const char *callback, int enabled)
{
    int rc = pcmk_ok;
    xmlNode *notify_msg = create_xml_node(NULL, __FUNCTION__);
    stonith_private_t *native = stonith->st_private;

    if (stonith->state != stonith_disconnected) {

        crm_xml_add(notify_msg, F_STONITH_OPERATION, T_STONITH_NOTIFY);
        if (enabled) {
            crm_xml_add(notify_msg, F_STONITH_NOTIFY_ACTIVATE, callback);
        } else {
            crm_xml_add(notify_msg, F_STONITH_NOTIFY_DEACTIVATE, callback);
        }

        rc = crm_ipc_send(native->ipc, notify_msg, crm_ipc_client_response, -1, NULL);
        if (rc < 0) {
            crm_perror(LOG_DEBUG, "Couldn't register for fencing notifications: %d", rc);
            rc = -ECOMM;
        } else {
            rc = pcmk_ok;
        }
    }

    free_xml(notify_msg);
    return rc;
}

static int
stonith_api_add_notification(stonith_t * stonith, const char *event,
                             void (*callback) (stonith_t * stonith, stonith_event_t * e))
{
    GList *list_item = NULL;
    stonith_notify_client_t *new_client = NULL;
    stonith_private_t *private = NULL;

    private = stonith->st_private;
    crm_trace("Adding callback for %s events (%d)", event, g_list_length(private->notify_list));

    new_client = calloc(1, sizeof(stonith_notify_client_t));
    new_client->event = event;
    new_client->notify = callback;

    list_item = g_list_find_custom(private->notify_list, new_client, stonithlib_GCompareFunc);

    if (list_item != NULL) {
        crm_warn("Callback already present");
        free(new_client);
        return -ENOTUNIQ;

    } else {
        private->notify_list = g_list_append(private->notify_list, new_client);

        stonith_set_notification(stonith, event, 1);

        crm_trace("Callback added (%d)", g_list_length(private->notify_list));
    }
    return pcmk_ok;
}

static int
stonith_api_del_notification(stonith_t * stonith, const char *event)
{
    GList *list_item = NULL;
    stonith_notify_client_t *new_client = NULL;
    stonith_private_t *private = NULL;

    crm_debug("Removing callback for %s events", event);

    private = stonith->st_private;
    new_client = calloc(1, sizeof(stonith_notify_client_t));
    new_client->event = event;
    new_client->notify = NULL;

    list_item = g_list_find_custom(private->notify_list, new_client, stonithlib_GCompareFunc);

    stonith_set_notification(stonith, event, 0);

    if (list_item != NULL) {
        stonith_notify_client_t *list_client = list_item->data;

        if (private->notify_refcnt) {
            list_client->delete = TRUE;
            private->notify_deletes = TRUE;
        } else {
            private->notify_list = g_list_remove(private->notify_list, list_client);
            free(list_client);
        }

        crm_trace("Removed callback");

    } else {
        crm_trace("Callback not present");
    }
    free(new_client);
    return pcmk_ok;
}

static int
stonith_api_add_callback(stonith_t * stonith, int call_id, int timeout, int options,
                         void *user_data, const char *callback_name,
                         void (*callback) (stonith_t * st, stonith_callback_data_t * data))
{
    stonith_callback_client_t *blob = NULL;
    stonith_private_t *private = NULL;

    CRM_CHECK(stonith != NULL, return -EINVAL);
    CRM_CHECK(stonith->st_private != NULL, return -EINVAL);
    private = stonith->st_private;

    if (call_id == 0) {
        private->op_callback = callback;

    } else if (call_id < 0) {
        if (!(options & st_opt_report_only_success)) {
            crm_trace("Call failed, calling %s: %s", callback_name, pcmk_strerror(call_id));
            invoke_callback(stonith, call_id, call_id, user_data, callback);
        } else {
            crm_warn("Fencer call failed: %s", pcmk_strerror(call_id));
        }
        return FALSE;
    }

    blob = calloc(1, sizeof(stonith_callback_client_t));
    blob->id = callback_name;
    blob->only_success = (options & st_opt_report_only_success) ? TRUE : FALSE;
    blob->user_data = user_data;
    blob->callback = callback;
    blob->allow_timeout_updates = (options & st_opt_timeout_updates) ? TRUE : FALSE;

    if (timeout > 0) {
        set_callback_timeout(blob, stonith, call_id, timeout);
    }

    g_hash_table_insert(private->stonith_op_callback_table, GINT_TO_POINTER(call_id), blob);
    crm_trace("Added callback to %s for call %d", callback_name, call_id);

    return TRUE;
}

static void
stonith_dump_pending_op(gpointer key, gpointer value, gpointer user_data)
{
    int call = GPOINTER_TO_INT(key);
    stonith_callback_client_t *blob = value;

    crm_debug("Call %d (%s): pending", call, crm_str(blob->id));
}

void
stonith_dump_pending_callbacks(stonith_t * stonith)
{
    stonith_private_t *private = stonith->st_private;

    if (private->stonith_op_callback_table == NULL) {
        return;
    }
    return g_hash_table_foreach(private->stonith_op_callback_table, stonith_dump_pending_op, NULL);
}

/*
 <notify t="st_notify" subt="st_device_register" st_op="st_device_register" st_rc="0" >
   <st_calldata >
     <stonith_command t="stonith-ng" st_async_id="088fb640-431a-48b9-b2fc-c4ff78d0a2d9" st_op="st_device_register" st_callid="2" st_callopt="4096" st_timeout="0" st_clientid="088fb640-431a-48b9-b2fc-c4ff78d0a2d9" st_clientname="cts-fence-helper" >
       <st_calldata >
         <st_device_id id="test-id" origin="create_device_registration_xml" agent="fence_virsh" namespace="stonith-ng" >
           <attributes ipaddr="localhost" pcmk-portmal="some-host=pcmk-1 pcmk-3=3,4" login="root" identity_file="/root/.ssh/id_dsa" />
         </st_device_id>
       </st_calldata>
     </stonith_command>
   </st_calldata>
 </notify>

 <notify t="st_notify" subt="st_notify_fence" st_op="st_notify_fence" st_rc="0" >
   <st_calldata >
     <st_notify_fence st_rc="0" st_target="some-host" st_op="st_fence" st_delegate="test-id" st_origin="61dd7759-e229-4be7-b1f8-ef49dd14d9f0" />
   </st_calldata>
 </notify>
*/
static stonith_event_t *
xml_to_event(xmlNode * msg)
{
    stonith_event_t *event = calloc(1, sizeof(stonith_event_t));
    const char *ntype = crm_element_value(msg, F_SUBTYPE);
    char *data_addr = crm_strdup_printf("//%s", ntype);
    xmlNode *data = get_xpath_object(data_addr, msg, LOG_DEBUG);

    crm_log_xml_trace(msg, "stonith_notify");

    crm_element_value_int(msg, F_STONITH_RC, &(event->result));

    if (safe_str_eq(ntype, T_STONITH_NOTIFY_FENCE)) {
        event->operation = crm_element_value_copy(msg, F_STONITH_OPERATION);

        if (data) {
            event->origin = crm_element_value_copy(data, F_STONITH_ORIGIN);
            event->action = crm_element_value_copy(data, F_STONITH_ACTION);
            event->target = crm_element_value_copy(data, F_STONITH_TARGET);
            event->executioner = crm_element_value_copy(data, F_STONITH_DELEGATE);
            event->id = crm_element_value_copy(data, F_STONITH_REMOTE_OP_ID);
            event->client_origin = crm_element_value_copy(data, F_STONITH_CLIENTNAME);
            event->device = crm_element_value_copy(data, F_STONITH_DEVICE);

        } else {
            crm_err("No data for %s event", ntype);
            crm_log_xml_notice(msg, "BadEvent");
        }
    }

    free(data_addr);
    return event;
}

static void
event_free(stonith_event_t * event)
{
    free(event->id);
    free(event->type);
    free(event->message);
    free(event->operation);
    free(event->origin);
    free(event->action);
    free(event->target);
    free(event->executioner);
    free(event->device);
    free(event->client_origin);
    free(event);
}

static void
stonith_send_notification(gpointer data, gpointer user_data)
{
    struct notify_blob_s *blob = user_data;
    stonith_notify_client_t *entry = data;
    stonith_event_t *st_event = NULL;
    const char *event = NULL;

    if (blob->xml == NULL) {
        crm_warn("Skipping callback - NULL message");
        return;
    }

    event = crm_element_value(blob->xml, F_SUBTYPE);

    if (entry == NULL) {
        crm_warn("Skipping callback - NULL callback client");
        return;

    } else if (entry->delete) {
        crm_trace("Skipping callback - marked for deletion");
        return;

    } else if (entry->notify == NULL) {
        crm_warn("Skipping callback - NULL callback");
        return;

    } else if (safe_str_neq(entry->event, event)) {
        crm_trace("Skipping callback - event mismatch %p/%s vs. %s", entry, entry->event, event);
        return;
    }

    st_event = xml_to_event(blob->xml);

    crm_trace("Invoking callback for %p/%s event...", entry, event);
    entry->notify(blob->stonith, st_event);
    crm_trace("Callback invoked...");

    event_free(st_event);
}

/*!
 * \internal
 * \brief Create and send an API request
 *
 * \param[in]  stonith       Stonith connection
 * \param[in]  op            API operation to request
 * \param[in]  data          Data to attach to request
 * \param[out] output_data   If not NULL, will be set to reply if synchronous
 * \param[in]  call_options  Bitmask of stonith_call_options to use
 * \param[in]  timeout       Error if not completed within this many seconds
 *
 * \return pcmk_ok (for synchronous requests) or positive call ID
 *         (for asynchronous requests) on success, -errno otherwise
 */
static int
stonith_send_command(stonith_t * stonith, const char *op, xmlNode * data, xmlNode ** output_data,
                     int call_options, int timeout)
{
    int rc = 0;
    int reply_id = -1;

    xmlNode *op_msg = NULL;
    xmlNode *op_reply = NULL;
    stonith_private_t *native = NULL;

    CRM_ASSERT(stonith && stonith->st_private && op);
    native = stonith->st_private;

    if (output_data != NULL) {
        *output_data = NULL;
    }

    if ((stonith->state == stonith_disconnected) || (native->token == NULL)) {
        return -ENOTCONN;
    }

    /* Increment the call ID, which must be positive to avoid conflicting with
     * error codes. This shouldn't be a problem unless the client mucked with
     * it or the counter wrapped around.
     */
    stonith->call_id++;
    if (stonith->call_id < 1) {
        stonith->call_id = 1;
    }

    op_msg = stonith_create_op(stonith->call_id, native->token, op, data, call_options);
    if (op_msg == NULL) {
        return -EINVAL;
    }

    crm_xml_add_int(op_msg, F_STONITH_TIMEOUT, timeout);
    crm_trace("Sending %s message to fencer with timeout %ds", op, timeout);

    {
        enum crm_ipc_flags ipc_flags = crm_ipc_flags_none;

        if (call_options & st_opt_sync_call) {
            ipc_flags |= crm_ipc_client_response;
        }
        rc = crm_ipc_send(native->ipc, op_msg, ipc_flags,
                          1000 * (timeout + 60), &op_reply);
    }
    free_xml(op_msg);

    if (rc < 0) {
        crm_perror(LOG_ERR, "Couldn't perform %s operation (timeout=%ds): %d", op, timeout, rc);
        rc = -ECOMM;
        goto done;
    }

    crm_log_xml_trace(op_reply, "Reply");

    if (!(call_options & st_opt_sync_call)) {
        crm_trace("Async call %d, returning", stonith->call_id);
        free_xml(op_reply);
        return stonith->call_id;
    }

    rc = pcmk_ok;
    crm_element_value_int(op_reply, F_STONITH_CALLID, &reply_id);

    if (reply_id == stonith->call_id) {
        crm_trace("Synchronous reply %d received", reply_id);

        if (crm_element_value_int(op_reply, F_STONITH_RC, &rc) != 0) {
            rc = -ENOMSG;
        }

        if ((call_options & st_opt_discard_reply) || output_data == NULL) {
            crm_trace("Discarding reply");

        } else {
            *output_data = op_reply;
            op_reply = NULL;    /* Prevent subsequent free */
        }

    } else if (reply_id <= 0) {
        crm_err("Received bad reply: No id set");
        crm_log_xml_err(op_reply, "Bad reply");
        free_xml(op_reply);
        rc = -ENOMSG;

    } else {
        crm_err("Received bad reply: %d (wanted %d)", reply_id, stonith->call_id);
        crm_log_xml_err(op_reply, "Old reply");
        free_xml(op_reply);
        rc = -ENOMSG;
    }

  done:
    if (crm_ipc_connected(native->ipc) == FALSE) {
        crm_err("Fencer disconnected");
        free(native->token); native->token = NULL;
        stonith->state = stonith_disconnected;
    }

    free_xml(op_reply);
    return rc;
}

/* Not used with mainloop */
bool
stonith_dispatch(stonith_t * st)
{
    gboolean stay_connected = TRUE;
    stonith_private_t *private = NULL;

    CRM_ASSERT(st != NULL);
    private = st->st_private;

    while (crm_ipc_ready(private->ipc)) {

        if (crm_ipc_read(private->ipc) > 0) {
            const char *msg = crm_ipc_buffer(private->ipc);

            stonith_dispatch_internal(msg, strlen(msg), st);
        }

        if (crm_ipc_connected(private->ipc) == FALSE) {
            crm_err("Connection closed");
            stay_connected = FALSE;
        }
    }

    return stay_connected;
}

static int
stonith_api_free(stonith_t * stonith)
{
    int rc = pcmk_ok;

    crm_trace("Destroying %p", stonith);

    if (stonith->state != stonith_disconnected) {
        crm_trace("Disconnecting %p first", stonith);
        rc = stonith->cmds->disconnect(stonith);
    }

    if (stonith->state == stonith_disconnected) {
        stonith_private_t *private = stonith->st_private;

        crm_trace("Removing %d callbacks", g_hash_table_size(private->stonith_op_callback_table));
        g_hash_table_destroy(private->stonith_op_callback_table);

        crm_trace("Destroying %d notification clients", g_list_length(private->notify_list));
        g_list_free_full(private->notify_list, free);

        free(stonith->st_private);
        free(stonith->cmds);
        free(stonith);

    } else {
        crm_err("Not free'ing active connection: %s (%d)", pcmk_strerror(rc), rc);
    }

    return rc;
}

void
stonith_api_delete(stonith_t * stonith)
{
    crm_trace("Destroying %p", stonith);
    if(stonith) {
        stonith->cmds->free(stonith);
    }
}

static int
stonith_api_validate(stonith_t *st, int call_options, const char *rsc_id,
                     const char *namespace_s, const char *agent,
                     stonith_key_value_t *params, int timeout, char **output,
                     char **error_output)
{
    /* Validation should be done directly via the agent, so we can get it from
     * stonith_admin when the cluster is not running, which is important for
     * higher-level tools.
     */

    int rc = pcmk_ok;

    /* Use a dummy node name in case the agent requires a target. We assume the
     * actual target doesn't matter for validation purposes (if in practice,
     * that is incorrect, we will need to allow the caller to pass the target).
     */
    const char *target = "node1";

    GHashTable *params_table = crm_str_table_new();

    // Convert parameter list to a hash table
    for (; params; params = params->next) {

        // Strip out Pacemaker-implemented parameters
        if (!crm_starts_with(params->key, "pcmk_")
                && strcmp(params->key, "provides")
                && strcmp(params->key, "stonith-timeout")) {
            g_hash_table_insert(params_table, strdup(params->key),
                                strdup(params->value));
        }
    }

#if SUPPORT_CIBSECRETS
    rc = replace_secret_params(rsc_id, params_table);
    if (rc < 0) {
        crm_warn("Could not replace secret parameters for validation of %s: %s",
                 agent, pcmk_strerror(rc));
    }
#endif

    if (output) {
        *output = NULL;
    }
    if (error_output) {
        *error_output = NULL;
    }

    switch (stonith_get_namespace(agent, namespace_s)) {
        case st_namespace_rhcs:
            rc = stonith__rhcs_validate(st, call_options, target, agent,
                                        params_table, timeout, output,
                                        error_output);
            break;

#if HAVE_STONITH_STONITH_H
        case st_namespace_lha:
            rc = stonith__lha_validate(st, call_options, target, agent,
                                       params_table, timeout, output,
                                       error_output);
            break;
#endif

        default:
            rc = -EINVAL;
            errno = EINVAL;
            crm_perror(LOG_ERR,
                       "Agent %s not found or does not support validation",
                       agent);
            break;
    }
    g_hash_table_destroy(params_table);
    return rc;
}

stonith_t *
stonith_api_new(void)
{
    stonith_t *new_stonith = NULL;
    stonith_private_t *private = NULL;

    new_stonith = calloc(1, sizeof(stonith_t));
    if (new_stonith == NULL) {
        return NULL;
    }

    private = calloc(1, sizeof(stonith_private_t));
    if (private == NULL) {
        free(new_stonith);
        return NULL;
    }
    new_stonith->st_private = private;

    private->stonith_op_callback_table = g_hash_table_new_full(g_direct_hash, g_direct_equal,
                                                               NULL, stonith_destroy_op_callback);
    private->notify_list = NULL;
    private->notify_refcnt = 0;
    private->notify_deletes = FALSE;

    new_stonith->call_id = 1;
    new_stonith->state = stonith_disconnected;

    new_stonith->cmds = calloc(1, sizeof(stonith_api_operations_t));
    if (new_stonith->cmds == NULL) {
        free(new_stonith->st_private);
        free(new_stonith);
        return NULL;
    }

/* *INDENT-OFF* */
    new_stonith->cmds->free       = stonith_api_free;
    new_stonith->cmds->connect    = stonith_api_signon;
    new_stonith->cmds->disconnect = stonith_api_signoff;

    new_stonith->cmds->list       = stonith_api_list;
    new_stonith->cmds->monitor    = stonith_api_monitor;
    new_stonith->cmds->status     = stonith_api_status;
    new_stonith->cmds->fence      = stonith_api_fence;
    new_stonith->cmds->confirm    = stonith_api_confirm;
    new_stonith->cmds->history    = stonith_api_history;

    new_stonith->cmds->list_agents  = stonith_api_device_list;
    new_stonith->cmds->metadata     = stonith_api_device_metadata;

    new_stonith->cmds->query           = stonith_api_query;
    new_stonith->cmds->remove_device   = stonith_api_remove_device;
    new_stonith->cmds->register_device = stonith_api_register_device;

    new_stonith->cmds->remove_level          = stonith_api_remove_level;
    new_stonith->cmds->remove_level_full     = stonith_api_remove_level_full;
    new_stonith->cmds->register_level        = stonith_api_register_level;
    new_stonith->cmds->register_level_full   = stonith_api_register_level_full;

    new_stonith->cmds->remove_callback       = stonith_api_del_callback;
    new_stonith->cmds->register_callback     = stonith_api_add_callback;
    new_stonith->cmds->remove_notification   = stonith_api_del_notification;
    new_stonith->cmds->register_notification = stonith_api_add_notification;

    new_stonith->cmds->validate              = stonith_api_validate;
/* *INDENT-ON* */

    return new_stonith;
}

/*!
 * \brief Make a blocking connection attempt to the fencer
 *
 * \param[in,out] st            Fencer API object
 * \param[in]     name          Client name to use with fencer
 * \param[in]     max_attempts  Return error if this many attempts fail
 *
 * \return pcmk_ok on success, result of last attempt otherwise
 */
int
stonith_api_connect_retry(stonith_t *st, const char *name, int max_attempts)
{
    int rc = -EINVAL; // if max_attempts is not positive

    for (int attempt = 1; attempt <= max_attempts; attempt++) {
        rc = st->cmds->connect(st, name, NULL);
        if (rc == pcmk_ok) {
            return pcmk_ok;
        } else if (attempt < max_attempts) {
            crm_notice("Fencer connection attempt %d of %d failed (retrying in 2s): %s "
                       CRM_XS " rc=%d",
                       attempt, max_attempts, pcmk_strerror(rc), rc);
            sleep(2);
        }
    }
    crm_notice("Could not connect to fencer: %s " CRM_XS " rc=%d",
               pcmk_strerror(rc), rc);
    return rc;
}

stonith_key_value_t *
stonith_key_value_add(stonith_key_value_t * head, const char *key, const char *value)
{
    stonith_key_value_t *p, *end;

    p = calloc(1, sizeof(stonith_key_value_t));
    if (key) {
        p->key = strdup(key);
    }
    if (value) {
        p->value = strdup(value);
    }

    end = head;
    while (end && end->next) {
        end = end->next;
    }

    if (end) {
        end->next = p;
    } else {
        head = p;
    }

    return head;
}

void
stonith_key_value_freeall(stonith_key_value_t * head, int keys, int values)
{
    stonith_key_value_t *p;

    while (head) {
        p = head->next;
        if (keys) {
            free(head->key);
        }
        if (values) {
            free(head->value);
        }
        free(head);
        head = p;
    }
}

#define api_log_open() openlog("stonith-api", LOG_CONS | LOG_NDELAY | LOG_PID, LOG_DAEMON)
#define api_log(level, fmt, args...) syslog(level, "%s: "fmt, __FUNCTION__, args)

int
stonith_api_kick(uint32_t nodeid, const char *uname, int timeout, bool off)
{
    int rc = pcmk_ok;
    stonith_t *st = stonith_api_new();
    const char *action = off? "off" : "reboot";

    api_log_open();
    if (st == NULL) {
        api_log(LOG_ERR, "API initialization failed, could not kick (%s) node %u/%s",
                action, nodeid, uname);
        return -EPROTO;
    }

    rc = st->cmds->connect(st, "stonith-api", NULL);
    if (rc != pcmk_ok) {
        api_log(LOG_ERR, "Connection failed, could not kick (%s) node %u/%s : %s (%d)",
                action, nodeid, uname, pcmk_strerror(rc), rc);
    } else {
        char *name = NULL;
        enum stonith_call_options opts = st_opt_sync_call | st_opt_allow_suicide;

        if (uname != NULL) {
            name = strdup(uname);
        } else if (nodeid > 0) {
            opts |= st_opt_cs_nodeid;
            name = crm_itoa(nodeid);
        }
        rc = st->cmds->fence(st, opts, name, action, timeout, 0);
        free(name);

        if (rc != pcmk_ok) {
            api_log(LOG_ERR, "Could not kick (%s) node %u/%s : %s (%d)",
                    action, nodeid, uname, pcmk_strerror(rc), rc);
        } else {
            api_log(LOG_NOTICE, "Node %u/%s kicked: %s", nodeid, uname, action);
        }
    }

    stonith_api_delete(st);
    return rc;
}

time_t
stonith_api_time(uint32_t nodeid, const char *uname, bool in_progress)
{
    int rc = pcmk_ok;
    time_t when = 0;
    stonith_t *st = stonith_api_new();
    stonith_history_t *history = NULL, *hp = NULL;

    if (st == NULL) {
        api_log(LOG_ERR, "Could not retrieve fence history for %u/%s: "
                "API initialization failed", nodeid, uname);
        return when;
    }

    rc = st->cmds->connect(st, "stonith-api", NULL);
    if (rc != pcmk_ok) {
        api_log(LOG_NOTICE, "Connection failed: %s (%d)", pcmk_strerror(rc), rc);
    } else {
        int entries = 0;
        int progress = 0;
        int completed = 0;
        char *name = NULL;
        enum stonith_call_options opts = st_opt_sync_call;

        if (uname != NULL) {
            name = strdup(uname);
        } else if (nodeid > 0) {
            opts |= st_opt_cs_nodeid;
            name = crm_itoa(nodeid);
        }
        rc = st->cmds->history(st, opts, name, &history, 120);
        free(name);

        for (hp = history; hp; hp = hp->next) {
            entries++;
            if (in_progress) {
                progress++;
                if (hp->state != st_done && hp->state != st_failed) {
                    when = time(NULL);
                }

            } else if (hp->state == st_done) {
                completed++;
                if (hp->completed > when) {
                    when = hp->completed;
                }
            }
        }

        stonith_history_free(history);

        if(rc == pcmk_ok) {
            api_log(LOG_INFO, "Found %d entries for %u/%s: %d in progress, %d completed", entries, nodeid, uname, progress, completed);
        } else {
            api_log(LOG_ERR, "Could not retrieve fence history for %u/%s: %s (%d)", nodeid, uname, pcmk_strerror(rc), rc);
        }
    }

    stonith_api_delete(st);

    if(when) {
        api_log(LOG_INFO, "Node %u/%s last kicked at: %ld", nodeid, uname, (long int)when);
    }
    return when;
}

bool
stonith_agent_exists(const char *agent, int timeout)
{
    stonith_t *st = NULL;
    stonith_key_value_t *devices = NULL;
    stonith_key_value_t *dIter = NULL;
    bool rc = FALSE;

    if (agent == NULL) {
        return rc;
    }

    st = stonith_api_new();
    if (st == NULL) {
        crm_err("Could not list fence agents: API memory allocation failed");
        return FALSE;
    }
    st->cmds->list_agents(st, st_opt_sync_call, NULL, &devices, timeout == 0 ? 120 : timeout);

    for (dIter = devices; dIter != NULL; dIter = dIter->next) {
        if (crm_str_eq(dIter->value, agent, TRUE)) {
            rc = TRUE;
            break;
        }
    }

    stonith_key_value_freeall(devices, 1, 1);
    stonith_api_delete(st);
    return rc;
}

const char *
stonith_action_str(const char *action)
{
    if (action == NULL) {
        return "fencing";
    } else if (!strcmp(action, "on")) {
        return "unfencing";
    } else if (!strcmp(action, "off")) {
        return "turning off";
    } else {
        return action;
    }
}

/*!
 * \internal
 * \brief Parse a target name from one line of a target list string
 *
 * \param[in]     line    One line of a target list string
 * \parma[in]     len     String length of line
 * \param[in,out] output  List to add newly allocated target name to
 */
static void
parse_list_line(const char *line, int len, GList **output)
{
    size_t i = 0;
    size_t entry_start = 0;

    /* Skip complaints about additional parameters device doesn't understand
     *
     * @TODO Document or eliminate the implied restriction of target names
     */
    if (strstr(line, "invalid") || strstr(line, "variable")) {
        crm_debug("Skipping list output line: %s", line);
        return;
    }

    // Process line content, character by character
    for (i = 0; i <= len; i++) {

        if (isspace(line[i]) || (line[i] == ',') || (line[i] == ';')
            || (line[i] == '\0')) {
            // We've found a separator (i.e. the end of an entry)

            int rc = 0;
            char *entry = NULL;

            if (i == entry_start) {
                // Skip leading and sequential separators
                entry_start = i + 1;
                continue;
            }

            entry = calloc(i - entry_start + 1, sizeof(char));
            CRM_ASSERT(entry != NULL);

            /* Read entry, stopping at first separator
             *
             * @TODO Document or eliminate these character restrictions
             */
            rc = sscanf(line + entry_start, "%[a-zA-Z0-9_-.]", entry);
            if (rc != 1) {
                crm_warn("Could not parse list output entry: %s "
                         CRM_XS " entry_start=%d position=%d",
                         line + entry_start, entry_start, i);
                free(entry);

            } else if (safe_str_eq(entry, "on") || safe_str_eq(entry, "off")) {
                /* Some agents print the target status in the list output,
                 * though none are known now (the separate list-status command
                 * is used for this, but it can also print "UNKNOWN"). To handle
                 * this possibility, skip such entries.
                 *
                 * @TODO Document or eliminate the implied restriction of target
                 * names.
                 */
                free(entry);

            } else {
                // We have a valid entry
                *output = g_list_append(*output, entry);
            }
            entry_start = i + 1;
        }
    }
}

/*!
 * \internal
 * \brief Parse a list of targets from a string
 *
 * \param[in] list_output  Target list as a string
 *
 * \return List of target names
 * \note The target list string format is flexible, to allow for user-specified
 *       lists such pcmk_host_list and the output of an agent's list action
 *       (whether direct or via the API, which escapes newlines). There may be
 *       multiple lines, separated by either a newline or an escaped newline
 *       (backslash n). Each line may have one or more target names, separated
 *       by any combination of whitespace, commas, and semi-colons. Lines
 *       containing "invalid" or "variable" will be ignored entirely. Target
 *       names "on" or "off" (case-insensitive) will be ignored. Target names
 *       may contain only alphanumeric characters, underbars (_), dashes (-),
 *       and dots (.) (if any other character occurs in the name, it and all
 *       subsequent characters in the name will be ignored).
 * \note The caller is responsible for freeing the result with
 *       g_list_free_full(result, free).
 */
GList *
stonith__parse_targets(const char *target_spec)
{
    GList *targets = NULL;

    if (target_spec != NULL) {
        size_t out_len = strlen(target_spec);
        size_t line_start = 0; // Starting index of line being processed

        for (size_t i = 0; i <= out_len; ++i) {
            if ((target_spec[i] == '\n') || (target_spec[i] == '\0')
                || ((target_spec[i] == '\\') && (target_spec[i + 1] == 'n'))) {
                // We've reached the end of one line of output

                int len = i - line_start;

                if (len > 0) {
                    char *line = strndup(target_spec + line_start, len);

                    line[len] = '\0'; // Because it might be a newline
                    parse_list_line(line, len, &targets);
                    free(line);
                }
                if (target_spec[i] == '\\') {
                    ++i; // backslash-n takes up two positions
                }
                line_start = i + 1;
            }
        }
    }
    return targets;
}

/*!
 * \internal
 * \brief Determine if a later stonith event succeeded.
 *
 * \note Before calling this function, use stonith__sort_history() to sort the
 *       top_history argument.
 */
gboolean
stonith__later_succeeded(stonith_history_t *event, stonith_history_t *top_history)
{
     gboolean ret = FALSE;

     for (stonith_history_t *prev_hp = top_history; prev_hp; prev_hp = prev_hp->next) {
        if (prev_hp == event) {
            break;
        }

         if ((prev_hp->state == st_done) &&
            safe_str_eq(event->target, prev_hp->target) &&
            safe_str_eq(event->action, prev_hp->action) &&
            safe_str_eq(event->delegate, prev_hp->delegate) &&
            (event->completed < prev_hp->completed)) {
            ret = TRUE;
            break;
        }
    }
    return ret;
}

/*!
 * \internal
 * \brief Sort the stonith-history
 *        sort by competed most current on the top
 *        pending actions lacking a completed-stamp are gathered at the top
 *
 * \param[in] history    List of stonith actions
 *
 */
stonith_history_t *
stonith__sort_history(stonith_history_t *history)
{
    stonith_history_t *new = NULL, *pending = NULL, *hp, *np, *tmp;

    for (hp = history; hp; ) {
        tmp = hp->next;
        if ((hp->state == st_done) || (hp->state == st_failed)) {
            /* sort into new */
            if ((!new) || (hp->completed > new->completed)) {
                hp->next = new;
                new = hp;
            } else {
                np = new;
                do {
                    if ((!np->next) || (hp->completed > np->next->completed)) {
                        hp->next = np->next;
                        np->next = hp;
                        break;
                    }
                    np = np->next;
                } while (1);
            }
        } else {
            /* put into pending */
            hp->next = pending;
            pending = hp;
        }
        hp = tmp;
    }

    /* pending actions don't have a completed-stamp so make them go front */
    if (pending) {
        stonith_history_t *last_pending = pending;

        while (last_pending->next) {
            last_pending = last_pending->next;
        }

        last_pending->next = new;
        new = pending;
    }
    return new;
}

// Deprecated functions kept only for backward API compatibility
const char *get_stonith_provider(const char *agent, const char *provider);

/*!
 * \brief Deprecated (use stonith_get_namespace() instead)
 */
const char *
get_stonith_provider(const char *agent, const char *provider)
{
    return stonith_namespace2text(stonith_get_namespace(agent, provider));
}

gboolean
stonith__string_in_list(GListPtr list, const char *item)
{
    GListPtr gIter;
    int lpc = 0;

    for (gIter = list; gIter != NULL; gIter = gIter->next) {
        const char *value = (const char *) gIter->data;

        if (safe_str_eq(item, value)) {
            return TRUE;
        } else {
            crm_trace("%d: '%s' != '%s'", lpc, item, value);
        }
        lpc++;
    }
    return FALSE;
}

gboolean
watchdog_fencing_enabled_for_node_api(stonith_t *st, const char *node)
{
    gboolean rv = FALSE;
    stonith_t *stonith_api = st?st:stonith_api_new();
    char *list = NULL;

    if(stonith_api) {
        if (stonith_api->state == stonith_disconnected) {
            int rc = stonith_api->cmds->connect(stonith_api, "stonith-api", NULL);

            if (rc != pcmk_ok) {
                crm_err("Failed connecting to Stonith-API for watchdog-fencing-query.");
            }
        }

        if (stonith_api->state != stonith_disconnected) {
            /* caveat!!!
             * this might fail when when stonithd is just updating the device-list
             * probably something we should fix as well for other api-calls */
            int rc = stonith_api->cmds->list(stonith_api, st_opt_sync_call, STONITH_WATCHDOG_ID, &list, 0);
            if ((rc != pcmk_ok) || (list == NULL)) {
                /* due to the race described above it can happen that
                 * we drop in here - so as not to make remote nodes
                 * panic on that answer
                 */
                crm_warn("watchdog-fencing-query failed");
            } else if (list[0] == '\0') {
	         crm_warn("watchdog-fencing-query returned an empty list - any node");
                rv = TRUE;
            } else {
                GListPtr targets = stonith__parse_targets(list);
                rv = stonith__string_in_list(targets, node);
                g_list_free_full(targets, free);
            }
            free(list);
            if (!st) {
                /* if we're provided the api we still might have done the
                 * connection - but let's assume the caller won't bother
                 */
                stonith_api->cmds->disconnect(stonith_api);
            }
        }

        if (!st) {
            stonith_api_delete(stonith_api);
        }
    } else {
        crm_err("Stonith-API for watchdog-fencing-query couldn't be created.");
    }
    crm_trace("Pacemaker assumes node %s %sto do watchdog-fencing.",
              node, rv?"":"not ");
    return rv;
}

gboolean
watchdog_fencing_enabled_for_node(const char *node)
{
    return watchdog_fencing_enabled_for_node_api(NULL, node);
}
