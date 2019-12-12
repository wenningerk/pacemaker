/*
 * Copyright 2004-2019 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU General Public License version 2
 * or later (GPLv2+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>

#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <crm/crm.h>
#include <crm/msg_xml.h>
#include <crm/pengine/rules.h>
#include <crm/cluster/internal.h>
#include <crm/cluster/election.h>
#include <crm/common/ipcs.h>

#include <crm/stonith-ng.h>
#include <crm/fencing/internal.h>

#include <pacemaker-controld.h>

qb_ipcs_service_t *ipcs = NULL;

#if SUPPORT_COROSYNC
extern gboolean crm_connect_corosync(crm_cluster_t * cluster);
#endif

void crm_shutdown(int nsig);
gboolean crm_read_options(gpointer user_data);

gboolean fsa_has_quorum = FALSE;
crm_trigger_t *fsa_source = NULL;
crm_trigger_t *config_read = NULL;
bool no_quorum_suicide_escalation = FALSE;

/*	 A_HA_CONNECT	*/
void
do_ha_control(long long action,
              enum crmd_fsa_cause cause,
              enum crmd_fsa_state cur_state,
              enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    gboolean registered = FALSE;
    static crm_cluster_t *cluster = NULL;

    if (cluster == NULL) {
        cluster = calloc(1, sizeof(crm_cluster_t));
    }

    if (action & A_HA_DISCONNECT) {
        crm_cluster_disconnect(cluster);
        crm_info("Disconnected from the cluster");

        set_bit(fsa_input_register, R_HA_DISCONNECTED);
    }

    if (action & A_HA_CONNECT) {
        crm_set_status_callback(&peer_update_callback);
        crm_set_autoreap(FALSE);

        if (is_corosync_cluster()) {
#if SUPPORT_COROSYNC
            registered = crm_connect_corosync(cluster);
#endif
        }

        if (registered == TRUE) {
            controld_election_init(cluster->uname);
            fsa_our_uname = cluster->uname;
            fsa_our_uuid = cluster->uuid;
            if(cluster->uuid == NULL) {
                crm_err("Could not obtain local uuid");
                registered = FALSE;
            }
        }

        if (registered == FALSE) {
            set_bit(fsa_input_register, R_HA_DISCONNECTED);
            register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
            return;
        }

        populate_cib_nodes(node_update_none, __FUNCTION__);
        clear_bit(fsa_input_register, R_HA_DISCONNECTED);
        crm_info("Connected to the cluster");
    }

    if (action & ~(A_HA_CONNECT | A_HA_DISCONNECT)) {
        crm_err("Unexpected action %s in %s", fsa_action2string(action), __FUNCTION__);
    }
}

/*	 A_SHUTDOWN	*/
void
do_shutdown(long long action,
            enum crmd_fsa_cause cause,
            enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    /* just in case */
    set_bit(fsa_input_register, R_SHUTDOWN);
    controld_disconnect_fencer(FALSE);
}

/*	 A_SHUTDOWN_REQ	*/
void
do_shutdown_req(long long action,
                enum crmd_fsa_cause cause,
                enum crmd_fsa_state cur_state,
                enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    xmlNode *msg = NULL;

    set_bit(fsa_input_register, R_SHUTDOWN);
    crm_info("Sending shutdown request to all peers (DC is %s)",
             (fsa_our_dc? fsa_our_dc : "not set"));
    msg = create_request(CRM_OP_SHUTDOWN_REQ, NULL, NULL, CRM_SYSTEM_CRMD, CRM_SYSTEM_CRMD, NULL);

/* 	set_bit(fsa_input_register, R_STAYDOWN); */
    if (send_cluster_message(NULL, crm_msg_crmd, msg, TRUE) == FALSE) {
        register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
    }
    free_xml(msg);
}

extern char *max_generation_from;
extern xmlNode *max_generation_xml;
extern GHashTable *resource_history;
extern GHashTable *voted;

void
crmd_fast_exit(crm_exit_t exit_code)
{
    if (is_set(fsa_input_register, R_STAYDOWN)) {
        crm_warn("Inhibiting respawn "CRM_XS" remapping exit code %d to %d",
                 exit_code, CRM_EX_FATAL);
        exit_code = CRM_EX_FATAL;

    } else if ((exit_code == CRM_EX_OK)
               && is_set(fsa_input_register, R_IN_RECOVERY)) {
        crm_err("Could not recover from internal error");
        exit_code = CRM_EX_ERROR;
    }
    crm_exit(exit_code);
}

crm_exit_t
crmd_exit(crm_exit_t exit_code)
{
    GListPtr gIter = NULL;
    GMainLoop *mloop = crmd_mainloop;

    static bool in_progress = FALSE;

    if (in_progress && (exit_code == CRM_EX_OK)) {
        crm_debug("Exit is already in progress");
        return exit_code;

    } else if(in_progress) {
        crm_notice("Error during shutdown process, exiting now with status %d (%s)",
                   exit_code, crm_exit_str(exit_code));
        crm_write_blackbox(SIGTRAP, NULL);
        crmd_fast_exit(exit_code);
    }

    in_progress = TRUE;
    crm_trace("Preparing to exit with status %d (%s)",
              exit_code, crm_exit_str(exit_code));

    /* Suppress secondary errors resulting from us disconnecting everything */
    set_bit(fsa_input_register, R_HA_DISCONNECTED);

/* Close all IPC servers and clients to ensure any and all shared memory files are cleaned up */

    if(ipcs) {
        crm_trace("Closing IPC server");
        mainloop_del_ipc_server(ipcs);
        ipcs = NULL;
    }

    controld_close_attrd_ipc();
    pe_subsystem_free();
    controld_disconnect_fencer(TRUE);

    if ((exit_code == CRM_EX_OK) && (crmd_mainloop == NULL)) {
        crm_debug("No mainloop detected");
        exit_code = CRM_EX_ERROR;
    }

    /* On an error, just get out.
     *
     * Otherwise, make the effort to have mainloop exit gracefully so
     * that it (mostly) cleans up after itself and valgrind has less
     * to report on - allowing real errors stand out
     */
    if (exit_code != CRM_EX_OK) {
        crm_notice("Forcing immediate exit with status %d (%s)",
                   exit_code, crm_exit_str(exit_code));
        crm_write_blackbox(SIGTRAP, NULL);
        crmd_fast_exit(exit_code);
    }

/* Clean up as much memory as possible for valgrind */

    for (gIter = fsa_message_queue; gIter != NULL; gIter = gIter->next) {
        fsa_data_t *fsa_data = gIter->data;

        crm_info("Dropping %s: [ state=%s cause=%s origin=%s ]",
                 fsa_input2string(fsa_data->fsa_input),
                 fsa_state2string(fsa_state),
                 fsa_cause2string(fsa_data->fsa_cause), fsa_data->origin);
        delete_fsa_input(fsa_data);
    }

    clear_bit(fsa_input_register, R_MEMBERSHIP);
    g_list_free(fsa_message_queue); fsa_message_queue = NULL;

    metadata_cache_fini();
    controld_election_fini();

    /* Tear down the CIB manager connection, but don't free it yet -- it could
     * be used when we drain the mainloop later.
     */
    cib_free_callbacks(fsa_cib_conn);
    fsa_cib_conn->cmds->signoff(fsa_cib_conn);

    verify_stopped(fsa_state, LOG_WARNING);
    clear_bit(fsa_input_register, R_LRM_CONNECTED);
    lrm_state_destroy_all();

    /* This basically will not work, since mainloop has a reference to it */
    mainloop_destroy_trigger(fsa_source); fsa_source = NULL;

    mainloop_destroy_trigger(config_read); config_read = NULL;
    mainloop_destroy_trigger(transition_trigger); transition_trigger = NULL;

    crm_client_cleanup();
    crm_peer_destroy();

    controld_free_fsa_timers();
    te_cleanup_stonith_history_sync(NULL, TRUE);
    controld_free_sched_timer();

    free(fsa_our_dc_version); fsa_our_dc_version = NULL;
    free(fsa_our_uname); fsa_our_uname = NULL;
    free(fsa_our_uuid); fsa_our_uuid = NULL;
    free(fsa_our_dc); fsa_our_dc = NULL;

    free(fsa_cluster_name); fsa_cluster_name = NULL;

    free(te_uuid); te_uuid = NULL;
    free(failed_stop_offset); failed_stop_offset = NULL;
    free(failed_start_offset); failed_start_offset = NULL;

    free(max_generation_from); max_generation_from = NULL;
    free_xml(max_generation_xml); max_generation_xml = NULL;

    mainloop_destroy_signal(SIGPIPE);
    mainloop_destroy_signal(SIGUSR1);
    mainloop_destroy_signal(SIGTERM);
    mainloop_destroy_signal(SIGTRAP);
    /* leave SIGCHLD engaged as we might still want to drain some service-actions */

    if (mloop) {
        GMainContext *ctx = g_main_loop_get_context(crmd_mainloop);

        /* Don't re-enter this block */
        crmd_mainloop = NULL;

        /* no signals on final draining anymore */
        mainloop_destroy_signal(SIGCHLD);

        crm_trace("Draining mainloop %d %d", g_main_loop_is_running(mloop), g_main_context_pending(ctx));

        {
            int lpc = 0;

            while((g_main_context_pending(ctx) && lpc < 10)) {
                lpc++;
                crm_trace("Iteration %d", lpc);
                g_main_context_dispatch(ctx);
            }
        }

        crm_trace("Closing mainloop %d %d", g_main_loop_is_running(mloop), g_main_context_pending(ctx));
        g_main_loop_quit(mloop);

        /* Won't do anything yet, since we're inside it now */
        g_main_loop_unref(mloop);
    } else {
        mainloop_destroy_signal(SIGCHLD);
    }

    cib_delete(fsa_cib_conn);
    fsa_cib_conn = NULL;

    throttle_fini();

    /* Graceful */
    crm_trace("Done preparing for exit with status %d (%s)",
              exit_code, crm_exit_str(exit_code));
    return exit_code;
}

/*	 A_EXIT_0, A_EXIT_1	*/
void
do_exit(long long action,
        enum crmd_fsa_cause cause,
        enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    crm_exit_t exit_code = CRM_EX_OK;
    int log_level = LOG_INFO;
    const char *exit_type = "gracefully";

    if (action & A_EXIT_1) {
        log_level = LOG_ERR;
        exit_type = "forcefully";
        exit_code = CRM_EX_ERROR;
    }

    verify_stopped(cur_state, LOG_ERR);
    do_crm_log(log_level, "Performing %s - %s exiting the controller",
               fsa_action2string(action), exit_type);

    crm_info("[%s] stopped (%d)", crm_system_name, exit_code);
    crmd_exit(exit_code);
}

static void sigpipe_ignore(int nsig) { return; }

/*	 A_STARTUP	*/
void
do_startup(long long action,
           enum crmd_fsa_cause cause,
           enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    crm_debug("Registering Signal Handlers");
    mainloop_add_signal(SIGTERM, crm_shutdown);
    mainloop_add_signal(SIGPIPE, sigpipe_ignore);

    fsa_source = mainloop_add_trigger(G_PRIORITY_HIGH, crm_fsa_trigger, NULL);
    config_read = mainloop_add_trigger(G_PRIORITY_HIGH, crm_read_options, NULL);
    transition_trigger = mainloop_add_trigger(G_PRIORITY_LOW, te_graph_trigger, NULL);

    crm_debug("Creating CIB manager and executor objects");
    fsa_cib_conn = cib_new();

    lrm_state_init_local();
    if (controld_init_fsa_timers() == FALSE) {
        register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
    }
}

static int32_t
crmd_ipc_accept(qb_ipcs_connection_t * c, uid_t uid, gid_t gid)
{
    crm_trace("Connection %p", c);
    if (crm_client_new(c, uid, gid) == NULL) {
        return -EIO;
    }
    return 0;
}

static void
crmd_ipc_created(qb_ipcs_connection_t * c)
{
    crm_trace("Connection %p", c);
}

static int32_t
crmd_ipc_dispatch(qb_ipcs_connection_t * c, void *data, size_t size)
{
    uint32_t id = 0;
    uint32_t flags = 0;
    crm_client_t *client = crm_client_get(c);

    xmlNode *msg = crm_ipcs_recv(client, data, size, &id, &flags);

    crm_trace("Invoked: %s", crm_client_name(client));
    crm_ipcs_send_ack(client, id, flags, "ack", __FUNCTION__, __LINE__);

    if (msg == NULL) {
        return 0;
    }

#if ENABLE_ACL
    CRM_ASSERT(client->user != NULL);
    crm_acl_get_set_user(msg, F_CRM_USER, client->user);
#endif

    crm_trace("Processing msg from %s", crm_client_name(client));
    crm_log_xml_trace(msg, "controller[inbound]");

    crm_xml_add(msg, F_CRM_SYS_FROM, client->id);
    if (crmd_authorize_message(msg, client, NULL)) {
        route_message(C_IPC_MESSAGE, msg);
    }

    trigger_fsa(fsa_source);
    free_xml(msg);
    return 0;
}

static int32_t
crmd_ipc_closed(qb_ipcs_connection_t * c)
{
    crm_client_t *client = crm_client_get(c);

    if (client) {
        crm_trace("Disconnecting %sregistered client %s (%p/%p)",
                  (client->userdata? "" : "un"), crm_client_name(client),
                  c, client);
        free(client->userdata);
        crm_client_destroy(client);
        trigger_fsa(fsa_source);
    }
    return 0;
}

static void
crmd_ipc_destroy(qb_ipcs_connection_t * c)
{
    crm_trace("Connection %p", c);
    crmd_ipc_closed(c);
}

/*	 A_STOP	*/
void
do_stop(long long action,
        enum crmd_fsa_cause cause,
        enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    crm_trace("Closing IPC server");
    mainloop_del_ipc_server(ipcs); ipcs = NULL;
    register_fsa_input(C_FSA_INTERNAL, I_TERMINATE, NULL);
}

/*	 A_STARTED	*/
void
do_started(long long action,
           enum crmd_fsa_cause cause,
           enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    static struct qb_ipcs_service_handlers crmd_callbacks = {
        .connection_accept = crmd_ipc_accept,
        .connection_created = crmd_ipc_created,
        .msg_process = crmd_ipc_dispatch,
        .connection_closed = crmd_ipc_closed,
        .connection_destroyed = crmd_ipc_destroy
    };

    if (cur_state != S_STARTING) {
        crm_err("Start cancelled... %s", fsa_state2string(cur_state));
        return;

    } else if (is_set(fsa_input_register, R_MEMBERSHIP) == FALSE) {
        crm_info("Delaying start, no membership data (%.16llx)", R_MEMBERSHIP);

        crmd_fsa_stall(TRUE);
        return;

    } else if (is_set(fsa_input_register, R_LRM_CONNECTED) == FALSE) {
        crm_info("Delaying start, not connected to executor (%.16llx)", R_LRM_CONNECTED);

        crmd_fsa_stall(TRUE);
        return;

    } else if (is_set(fsa_input_register, R_CIB_CONNECTED) == FALSE) {
        crm_info("Delaying start, CIB not connected (%.16llx)", R_CIB_CONNECTED);

        crmd_fsa_stall(TRUE);
        return;

    } else if (is_set(fsa_input_register, R_READ_CONFIG) == FALSE) {
        crm_info("Delaying start, Config not read (%.16llx)", R_READ_CONFIG);

        crmd_fsa_stall(TRUE);
        return;

    } else if (is_set(fsa_input_register, R_PEER_DATA) == FALSE) {

        crm_info("Delaying start, No peer data (%.16llx)", R_PEER_DATA);
        crmd_fsa_stall(TRUE);
        return;
    }

    crm_debug("Init server comms");
    ipcs = crmd_ipc_server_init(&crmd_callbacks);
    if (ipcs == NULL) {
        crm_err("Failed to create IPC server: shutting down and inhibiting respawn");
        register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
    } else {
        crm_notice("Pacemaker controller successfully started and accepting connections");
    }
    controld_trigger_fencer_connect();

    clear_bit(fsa_input_register, R_STARTING);
    register_fsa_input(msg_data->fsa_cause, I_PENDING, NULL);
}

/*	 A_RECOVER	*/
void
do_recover(long long action,
           enum crmd_fsa_cause cause,
           enum crmd_fsa_state cur_state, enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    set_bit(fsa_input_register, R_IN_RECOVERY);
    crm_warn("Fast-tracking shutdown in response to errors");

    register_fsa_input(C_FSA_INTERNAL, I_TERMINATE, NULL);
}

/* *INDENT-OFF* */
static pe_cluster_option crmd_opts[] = {
	/* name, old-name, validate, values, default, short description, long description */
	{ "dc-version", NULL, "string", NULL, "none", NULL,
          "Version of Pacemaker on the cluster's DC.",
          "Includes the hash which identifies the exact changeset it was built from.  Used for diagnostic purposes."
        },
	{ "cluster-infrastructure", NULL, "string", NULL, "corosync", NULL,
          "The messaging stack on which Pacemaker is currently running.",
          "Used for informational and diagnostic purposes." },
    { "cluster-name", NULL, "string", NULL, NULL, NULL,
        "An arbitrary name for the cluster",
        "This optional value is mostly for users' convenience as desired "
        "in administration, but may also be used in Pacemaker configuration "
        "rules via the #cluster-name node attribute, and by higher-level tools "
        "and resource agents."
    },
	{ XML_CONFIG_ATTR_DC_DEADTIME, NULL, "time", NULL, "20s", &check_time,
          "How long to wait for a response from other nodes during startup.",
          "The \"correct\" value will depend on the speed/load of your network and the type of switches used."
        },
	{ XML_CONFIG_ATTR_RECHECK, NULL, "time",
	  "Zero disables polling.  Positive values are an interval in seconds (unless other SI units are specified. eg. 5min)",
          "15min", &check_timer,
	  "Polling interval for time based changes to options, resource parameters and constraints.",
	  "The Cluster is primarily event driven, however the configuration can have elements that change based on time."
	  "  To ensure these changes take effect, we can optionally poll the cluster's status for changes."
        },

	{ "load-threshold", NULL, "percentage", NULL, "80%", &check_utilization,
	  "The maximum amount of system resources that should be used by nodes in the cluster",
	  "The cluster will slow down its recovery process when the amount of system resources used"
          " (currently CPU) approaches this limit",
        },
	{ "node-action-limit", NULL, "integer", NULL, "0", &check_number,
          "The maximum number of jobs that can be scheduled per node. Defaults to 2x cores"},
    { XML_CONFIG_ATTR_FENCE_REACTION, NULL, "string", NULL, "stop", NULL,
        "How a cluster node should react if notified of its own fencing",
        "A cluster node may receive notification of its own fencing if fencing "
        "is misconfigured, or if fabric fencing is in use that doesn't cut "
        "cluster communication. Allowed values are \"stop\" to attempt to "
        "immediately stop pacemaker and stay stopped, or \"panic\" to attempt "
        "to immediately reboot the local node, falling back to stop on failure."
    },
	{ XML_CONFIG_ATTR_ELECTION_FAIL, NULL, "time", NULL, "2min", &check_timer,
          "*** Advanced Use Only ***.", "If need to adjust this value, it probably indicates the presence of a bug."
        },
	{ XML_CONFIG_ATTR_FORCE_QUIT, NULL, "time", NULL, "20min", &check_timer,
          "*** Advanced Use Only ***.", "If need to adjust this value, it probably indicates the presence of a bug."
        },
	{
        "join-integration-timeout", "crmd-integration-timeout",
        "time", NULL, "3min", &check_timer,
        "*** Advanced Use Only ***",
        "If need to adjust this value, it probably indicates the presence of a bug"
    },
	{
        "join-finalization-timeout", "crmd-finalization-timeout",
        "time", NULL, "30min", &check_timer,
        "*** Advanced Use Only ***",
        "If you need to adjust this value, it probably indicates the presence of a bug"
    },
	{
        "transition-delay", "crmd-transition-delay",
        "time", NULL, "0s", &check_timer,
        "*** Advanced Use Only *** Enabling this option will slow down cluster recovery under all conditions",
        "Delay cluster recovery for the configured interval to allow for additional/related events to occur.\n"
        "Useful if your configuration is sensitive to the order in which ping updates arrive."
    },
	{ "stonith-watchdog-timeout", NULL, "time", NULL, NULL, &controld_verify_stonith_watchdog_timeout,
	  "How long to wait before we can assume nodes are safely down", NULL
        },
        { "stonith-max-attempts",NULL,"integer",NULL,"10",&check_positive_number,
          "How many times stonith can fail before it will no longer be attempted on a target"
        },   
	{ "no-quorum-policy", NULL, "enum", "stop, freeze, ignore, suicide", "stop", &check_quorum, NULL, NULL },
};
/* *INDENT-ON* */

void
crmd_metadata(void)
{
    config_metadata("pacemaker-controld", "1.0",
                    "controller properties",
                    "Cluster properties used by Pacemaker's controller,"
                    " formerly known as crmd",
                    crmd_opts, DIMOF(crmd_opts));
}

static void
verify_crmd_options(GHashTable * options)
{
    verify_all_options(options, crmd_opts, DIMOF(crmd_opts));
}

static const char *
crmd_pref(GHashTable * options, const char *name)
{
    return get_cluster_pref(options, crmd_opts, DIMOF(crmd_opts), name);
}

static void
config_query_callback(xmlNode * msg, int call_id, int rc, xmlNode * output, void *user_data)
{
    const char *value = NULL;
    GHashTable *config_hash = NULL;
    crm_time_t *now = crm_time_new(NULL);
    xmlNode *crmconfig = NULL;
    xmlNode *alerts = NULL;

    if (rc != pcmk_ok) {
        fsa_data_t *msg_data = NULL;

        crm_err("Local CIB query resulted in an error: %s", pcmk_strerror(rc));
        register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);

        if (rc == -EACCES || rc == -pcmk_err_schema_validation) {
            crm_err("The cluster is mis-configured - shutting down and staying down");
            set_bit(fsa_input_register, R_STAYDOWN);
        }
        goto bail;
    }

    crmconfig = output;
    if ((crmconfig) &&
        (crm_element_name(crmconfig)) &&
        (strcmp(crm_element_name(crmconfig), XML_CIB_TAG_CRMCONFIG) != 0)) {
        crmconfig = first_named_child(crmconfig, XML_CIB_TAG_CRMCONFIG);
    }
    if (!crmconfig) {
        fsa_data_t *msg_data = NULL;

        crm_err("Local CIB query for " XML_CIB_TAG_CRMCONFIG " section failed");
        register_fsa_error(C_FSA_INTERNAL, I_ERROR, NULL);
        goto bail;
    }

    crm_debug("Call %d : Parsing CIB options", call_id);
    config_hash = crm_str_table_new();
    pe_unpack_nvpairs(crmconfig, crmconfig, XML_CIB_TAG_PROPSET, NULL,
                      config_hash, CIB_OPTIONS_FIRST, FALSE, now, NULL);

    verify_crmd_options(config_hash);

    value = crmd_pref(config_hash, XML_CONFIG_ATTR_DC_DEADTIME);
    election_trigger->period_ms = crm_parse_interval_spec(value);

    value = crmd_pref(config_hash, "node-action-limit"); /* Also checks migration-limit */
    throttle_update_job_max(value);

    value = crmd_pref(config_hash, "load-threshold");
    if(value) {
        throttle_set_load_target(strtof(value, NULL) / 100.0);
    }

    value = crmd_pref(config_hash, "no-quorum-policy");
    if (safe_str_eq(value, "suicide") && pcmk_locate_sbd()) {
        no_quorum_suicide_escalation = TRUE;
    }

    set_fence_reaction(crmd_pref(config_hash, XML_CONFIG_ATTR_FENCE_REACTION));

    value = crmd_pref(config_hash,"stonith-max-attempts");
    update_stonith_max_attempts(value);

    value = crmd_pref(config_hash, XML_CONFIG_ATTR_FORCE_QUIT);
    shutdown_escalation_timer->period_ms = crm_parse_interval_spec(value);
    crm_debug("Shutdown escalation occurs if DC has not responded to request in %ums",
              shutdown_escalation_timer->period_ms);

    value = crmd_pref(config_hash, XML_CONFIG_ATTR_ELECTION_FAIL);
    controld_set_election_period(value);

    value = crmd_pref(config_hash, XML_CONFIG_ATTR_RECHECK);
    recheck_interval_ms = crm_parse_interval_spec(value);
    crm_debug("Re-run scheduler after %dms of inactivity", recheck_interval_ms);

    value = crmd_pref(config_hash, "transition-delay");
    transition_timer->period_ms = crm_parse_interval_spec(value);

    value = crmd_pref(config_hash, "join-integration-timeout");
    integration_timer->period_ms = crm_parse_interval_spec(value);

    value = crmd_pref(config_hash, "join-finalization-timeout");
    finalization_timer->period_ms = crm_parse_interval_spec(value);

    free(fsa_cluster_name);
    fsa_cluster_name = NULL;

    value = g_hash_table_lookup(config_hash, "cluster-name");
    if (value) {
        fsa_cluster_name = strdup(value);
    }

    alerts = first_named_child(output, XML_CIB_TAG_ALERTS);
    crmd_unpack_alerts(alerts);

    set_bit(fsa_input_register, R_READ_CONFIG);
    crm_trace("Triggering FSA: %s", __FUNCTION__);
    mainloop_set_trigger(fsa_source);

    g_hash_table_destroy(config_hash);
  bail:
    crm_time_free(now);
}

gboolean
crm_read_options(gpointer user_data)
{
    int call_id =
        fsa_cib_conn->cmds->query(fsa_cib_conn,
            "//" XML_CIB_TAG_CRMCONFIG " | //" XML_CIB_TAG_ALERTS,
            NULL, cib_xpath | cib_scope_local);

    fsa_register_cib_callback(call_id, FALSE, NULL, config_query_callback);
    crm_trace("Querying the CIB... call %d", call_id);
    return TRUE;
}

/*	 A_READCONFIG	*/
void
do_read_config(long long action,
               enum crmd_fsa_cause cause,
               enum crmd_fsa_state cur_state,
               enum crmd_fsa_input current_input, fsa_data_t * msg_data)
{
    throttle_init();
    mainloop_set_trigger(config_read);
}

void
crm_shutdown(int nsig)
{
    if (crmd_mainloop != NULL && g_main_loop_is_running(crmd_mainloop)) {
        if (is_set(fsa_input_register, R_SHUTDOWN)) {
            crm_err("Escalating the shutdown");
            register_fsa_input_before(C_SHUTDOWN, I_ERROR, NULL);

        } else {
            set_bit(fsa_input_register, R_SHUTDOWN);
            register_fsa_input(C_SHUTDOWN, I_SHUTDOWN, NULL);

            if (shutdown_escalation_timer->period_ms == 0) {
                const char *value = crmd_pref(NULL, XML_CONFIG_ATTR_FORCE_QUIT);

                shutdown_escalation_timer->period_ms = crm_parse_interval_spec(value);
            }

            /* can't rely on this... */
            crm_notice("Shutting down cluster resource manager " CRM_XS
                       " limit=%ums", shutdown_escalation_timer->period_ms);
            controld_start_timer(shutdown_escalation_timer);
        }

    } else {
        crm_info("exit from shutdown");
        crmd_exit(CRM_EX_OK);
    }
}
