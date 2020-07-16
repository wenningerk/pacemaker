/*
 * Copyright 2010-2020 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU General Public License version 2
 * or later (GPLv2+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>
#include "pacemakerd.h"

#include <pwd.h>
#include <grp.h>
#include <poll.h>
#include <stdio.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/reboot.h>

#include <crm/crm.h>  /* indirectly: CRM_EX_* */
#include <crm/cib/internal.h>  /* cib_channel_ro */
#include <crm/msg_xml.h>
#include <crm/common/ipc_internal.h>
#include <crm/common/mainloop.h>
#include <crm/cluster/internal.h>
#include <crm/cluster.h>

#ifdef SUPPORT_COROSYNC
#include <corosync/cfg.h>
#endif

#include <dirent.h>
#include <ctype.h>

static gboolean pcmk_quorate = FALSE;
static gboolean fatal_error = FALSE;
static GMainLoop *mainloop = NULL;
static bool global_keep_tracking = false;

#define PCMK_PROCESS_CHECK_INTERVAL 5

static const char *local_name = NULL;
static uint32_t local_nodeid = 0;
static crm_trigger_t *shutdown_trigger = NULL;
static crm_trigger_t *startup_trigger = NULL;
static const char *pid_file = PCMK_RUN_DIR "/pacemaker.pid";

static const char *pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_INIT;
static gboolean running_with_sbd = FALSE;
static uint shutdown_complete_state_reported_to = 0;
static gboolean shutdown_complete_state_reported_client_closed = FALSE;

typedef struct pcmk_child_s {
    pid_t pid;
    long flag;
    int start_seq;
    int respawn_count;
    gboolean respawn;
    const char *name;
    const char *uid;
    const char *command;
    const char *endpoint;  /* IPC server name */

    gboolean active_before_startup;
} pcmk_child_t;

/* Index into the array below */
#define PCMK_CHILD_CONTROLD  3

static pcmk_child_t pcmk_children[] = {
    {
        0, crm_proc_none,       0, 0, FALSE, "none",
        NULL, NULL
    },
    {
        0, crm_proc_execd,      3, 0, TRUE,  "pacemaker-execd",
        NULL, CRM_DAEMON_DIR "/pacemaker-execd",
        CRM_SYSTEM_LRMD
    },
    {
        0, crm_proc_based,      1, 0, TRUE,  "pacemaker-based",
        CRM_DAEMON_USER, CRM_DAEMON_DIR "/pacemaker-based",
        PCMK__SERVER_BASED_RO
    },
    {
        0, crm_proc_controld,   6, 0, TRUE, "pacemaker-controld",
        CRM_DAEMON_USER, CRM_DAEMON_DIR "/pacemaker-controld",
        CRM_SYSTEM_CRMD
    },
    {
        0, crm_proc_attrd,      4, 0, TRUE, "pacemaker-attrd",
        CRM_DAEMON_USER, CRM_DAEMON_DIR "/pacemaker-attrd",
        T_ATTRD
    },
    {
        0, crm_proc_schedulerd, 5, 0, TRUE, "pacemaker-schedulerd",
        CRM_DAEMON_USER, CRM_DAEMON_DIR "/pacemaker-schedulerd",
        CRM_SYSTEM_PENGINE
    },
    {
        0, crm_proc_fenced,     2, 0, TRUE, "pacemaker-fenced",
        NULL, CRM_DAEMON_DIR "/pacemaker-fenced",
        "stonith-ng"
    },
};

static gboolean check_active_before_startup_processes(gpointer user_data);
static int child_liveness(pcmk_child_t *child);
static gboolean start_child(pcmk_child_t * child);
static gboolean update_node_processes(uint32_t id, const char *uname,
                                      uint32_t procs);
void update_process_clients(pcmk__client_t *client);

static uint32_t
get_process_list(void)
{
    int lpc = 0;
    uint32_t procs = crm_get_cluster_proc();

    for (lpc = 0; lpc < SIZEOF(pcmk_children); lpc++) {
        if (pcmk_children[lpc].pid != 0) {
            procs |= pcmk_children[lpc].flag;
        }
    }
    return procs;
}

static void
pcmk_process_exit(pcmk_child_t * child)
{
    child->pid = 0;
    child->active_before_startup = FALSE;

    /* Broadcast the fact that one of our processes died ASAP
     *
     * Try to get some logging of the cause out first though
     * because we're probably about to get fenced
     *
     * Potentially do this only if respawn_count > N
     * to allow for local recovery
     */
    update_node_processes(local_nodeid, NULL, get_process_list());

    child->respawn_count += 1;
    if (child->respawn_count > MAX_RESPAWN) {
        crm_err("Child respawn count exceeded by %s", child->name);
        child->respawn = FALSE;
    }

    if (shutdown_trigger) {
        /* resume step-wise shutdown (returned TRUE yields no parallelizing) */
        mainloop_set_trigger(shutdown_trigger);
        /* intended to speed up propagating expected lay-off of the daemons? */
        update_node_processes(local_nodeid, NULL, get_process_list());

    } else if (!child->respawn) {
        /* nothing to do */

    } else if (crm_is_true(getenv("PCMK_fail_fast"))) {
        crm_err("Rebooting system because of %s", child->name);
        pcmk_panic(__FUNCTION__);

    } else if (child_liveness(child) == pcmk_rc_ok) {
        crm_warn("One-off suppressing strict respawning of a child process %s,"
                 " appears alright per %s IPC end-point",
                 child->name, child->endpoint);
        /* need to monitor how it evolves, and start new process if badly */
        child->active_before_startup = TRUE;
        if (!global_keep_tracking) {
            global_keep_tracking = true;
            g_timeout_add_seconds(PCMK_PROCESS_CHECK_INTERVAL,
                                  check_active_before_startup_processes, NULL);
        }

    } else {
        crm_notice("Respawning failed child process: %s", child->name);
        start_child(child);
    }
}

static void pcmk_exit_with_cluster(int exitcode)
{
#ifdef SUPPORT_COROSYNC
    corosync_cfg_handle_t cfg_handle;
    cs_error_t err;

    if (exitcode == CRM_EX_FATAL) {
	    crm_info("Asking Corosync to shut down");
	    err = corosync_cfg_initialize(&cfg_handle, NULL);
	    if (err != CS_OK) {
		    crm_warn("Unable to open handle to corosync to close it down. err=%d", err);
	    }
	    err = corosync_cfg_try_shutdown(cfg_handle, COROSYNC_CFG_SHUTDOWN_FLAG_IMMEDIATE);
	    if (err != CS_OK) {
		    crm_warn("Corosync shutdown failed. err=%d", err);
	    }
	    corosync_cfg_finalize(cfg_handle);
    }
#endif
    crm_exit(exitcode);
}

static void
pcmk_child_exit(mainloop_child_t * p, pid_t pid, int core, int signo, int exitcode)
{
    pcmk_child_t *child = mainloop_child_userdata(p);
    const char *name = mainloop_child_name(p);

    if (signo) {
        do_crm_log(((signo == SIGKILL)? LOG_WARNING : LOG_ERR),
                   "%s[%d] terminated with signal %d (core=%d)",
                   name, pid, signo, core);

    } else {
        switch(exitcode) {
            case CRM_EX_OK:
                crm_info("%s[%d] exited with status %d (%s)",
                         name, pid, exitcode, crm_exit_str(exitcode));
                break;

            case CRM_EX_FATAL:
                crm_warn("Shutting cluster down because %s[%d] had fatal failure",
                         name, pid);
                child->respawn = FALSE;
                fatal_error = TRUE;
                pcmk_shutdown(SIGTERM);
                break;

            case CRM_EX_PANIC:
                crm_emerg("%s[%d] instructed the machine to reset", name, pid);
                child->respawn = FALSE;
                fatal_error = TRUE;
                pcmk_panic(__FUNCTION__);
                pcmk_shutdown(SIGTERM);
                break;

            default:
                crm_err("%s[%d] exited with status %d (%s)",
                        name, pid, exitcode, crm_exit_str(exitcode));
                break;
        }
    }

    pcmk_process_exit(child);
}

static gboolean
stop_child(pcmk_child_t * child, int signal)
{
    if (signal == 0) {
        signal = SIGTERM;
    }

    /* why to skip PID of 1?
       - FreeBSD ~ how untrackable process behind IPC is masqueraded as
       - elsewhere: how "init" task is designated; in particular, in systemd
         arrangement of socket-based activation, this is pretty real */
    if (child->command == NULL || child->pid == PCMK__SPECIAL_PID) {
        crm_debug("Nothing to do for child \"%s\" (process %lld)",
                  child->name, (long long) PCMK__SPECIAL_PID_AS_0(child->pid));
        return TRUE;
    }

    if (child->pid <= 0) {
        crm_trace("Client %s not running", child->name);
        return TRUE;
    }

    errno = 0;
    if (kill(child->pid, signal) == 0) {
        crm_notice("Stopping %s "CRM_XS" sent signal %d to process %lld",
                   child->name, signal, (long long) child->pid);

    } else {
        crm_err("Could not stop %s (process %lld) with signal %d: %s",
                child->name, (long long) child->pid, signal, strerror(errno));
    }

    return TRUE;
}

static char *opts_default[] = { NULL, NULL };
static char *opts_vgrind[] = { NULL, NULL, NULL, NULL, NULL };

/* TODO once libqb is taught to juggle with IPC end-points carried over as
        bare file descriptor (https://github.com/ClusterLabs/libqb/issues/325)
        it shall hand over these descriptors here if/once they are successfully
        pre-opened in (presumably) child_liveness(), to avoid any remaining
        room for races */
static gboolean
start_child(pcmk_child_t * child)
{
    uid_t uid = 0;
    gid_t gid = 0;
    gboolean use_valgrind = FALSE;
    gboolean use_callgrind = FALSE;
    const char *env_valgrind = getenv("PCMK_valgrind_enabled");
    const char *env_callgrind = getenv("PCMK_callgrind_enabled");

    child->active_before_startup = FALSE;

    if (child->command == NULL) {
        crm_info("Nothing to do for child \"%s\"", child->name);
        return TRUE;
    }

    if (env_callgrind != NULL && crm_is_true(env_callgrind)) {
        use_callgrind = TRUE;
        use_valgrind = TRUE;

    } else if (env_callgrind != NULL && strstr(env_callgrind, child->name)) {
        use_callgrind = TRUE;
        use_valgrind = TRUE;

    } else if (env_valgrind != NULL && crm_is_true(env_valgrind)) {
        use_valgrind = TRUE;

    } else if (env_valgrind != NULL && strstr(env_valgrind, child->name)) {
        use_valgrind = TRUE;
    }

    if (use_valgrind && strlen(VALGRIND_BIN) == 0) {
        crm_warn("Cannot enable valgrind for %s:"
                 " The location of the valgrind binary is unknown", child->name);
        use_valgrind = FALSE;
    }

    if (child->uid) {
        if (crm_user_lookup(child->uid, &uid, &gid) < 0) {
            crm_err("Invalid user (%s) for %s: not found", child->uid, child->name);
            return FALSE;
        }
        crm_info("Using uid=%u and group=%u for process %s", uid, gid, child->name);
    }

    child->pid = fork();
    CRM_ASSERT(child->pid != -1);

    if (child->pid > 0) {
        /* parent */
        mainloop_child_add(child->pid, 0, child->name, child, pcmk_child_exit);

        crm_info("Forked child %lld for process %s%s",
                 (long long) child->pid, child->name,
                 use_valgrind ? " (valgrind enabled: " VALGRIND_BIN ")" : "");
        update_node_processes(local_nodeid, NULL, get_process_list());
        return TRUE;

    } else {
        /* Start a new session */
        (void)setsid();

        /* Setup the two alternate arg arrays */
        opts_vgrind[0] = strdup(VALGRIND_BIN);
        if (use_callgrind) {
            opts_vgrind[1] = strdup("--tool=callgrind");
            opts_vgrind[2] = strdup("--callgrind-out-file=" CRM_STATE_DIR "/callgrind.out.%p");
            opts_vgrind[3] = strdup(child->command);
            opts_vgrind[4] = NULL;
        } else {
            opts_vgrind[1] = strdup(child->command);
            opts_vgrind[2] = NULL;
            opts_vgrind[3] = NULL;
            opts_vgrind[4] = NULL;
        }
        opts_default[0] = strdup(child->command);

        if(gid) {
            // Whether we need root group access to talk to cluster layer
            bool need_root_group = TRUE;

            if (is_corosync_cluster()) {
                /* Corosync clusters can drop root group access, because we set
                 * uidgid.gid.${gid}=1 via CMAP, which allows these processes to
                 * connect to corosync.
                 */
                need_root_group = FALSE;
            }

            // Drop root group access if not needed
            if (!need_root_group && (setgid(gid) < 0)) {
                crm_perror(LOG_ERR, "Could not set group to %d", gid);
            }

            /* Initialize supplementary groups to only those always granted to
             * the user, plus haclient (so we can access IPC).
             */
            if (initgroups(child->uid, gid) < 0) {
                crm_err("Cannot initialize groups for %s: %s (%d)", child->uid, pcmk_strerror(errno), errno);
            }
        }

        if (uid && setuid(uid) < 0) {
            crm_perror(LOG_ERR, "Could not set user to %d (%s)", uid, child->uid);
        }

        pcmk__close_fds_in_child(true);

        pcmk__open_devnull(O_RDONLY);   // stdin (fd 0)
        pcmk__open_devnull(O_WRONLY);   // stdout (fd 1)
        pcmk__open_devnull(O_WRONLY);   // stderr (fd 2)

        if (use_valgrind) {
            (void)execvp(VALGRIND_BIN, opts_vgrind);
        } else {
            (void)execvp(child->command, opts_default);
        }
        crm_perror(LOG_ERR, "FATAL: Cannot exec %s", child->command);
        crm_exit(CRM_EX_FATAL);
    }
    return TRUE;                /* never reached */
}

static gboolean
escalate_shutdown(gpointer data)
{

    pcmk_child_t *child = data;

    if (child->pid == PCMK__SPECIAL_PID) {
        pcmk_process_exit(child);

    } else if (child->pid != 0) {
        /* Use SIGSEGV instead of SIGKILL to create a core so we can see what it was up to */
        crm_err("Child %s not terminating in a timely manner, forcing", child->name);
        stop_child(child, SIGSEGV);
    }
    return FALSE;
}

#define SHUTDOWN_ESCALATION_PERIOD 180000  /* 3m */

static gboolean
pcmk_shutdown_worker(gpointer user_data)
{
    static int phase = SIZEOF(pcmk_children);
    static time_t next_log = 0;

    int lpc = 0;

    if (phase == SIZEOF(pcmk_children)) {
        crm_notice("Shutting down Pacemaker");
        pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_SHUTTINGDOWN;
    }

    for (; phase > 0; phase--) {
        /* Don't stop anything with start_seq < 1 */

        for (lpc = SIZEOF(pcmk_children) - 1; lpc >= 0; lpc--) {
            pcmk_child_t *child = &(pcmk_children[lpc]);

            if (phase != child->start_seq) {
                continue;
            }

            if (child->pid != 0) {
                time_t now = time(NULL);

                if (child->respawn) {
                    if (child->pid == PCMK__SPECIAL_PID) {
                        crm_warn("The process behind %s IPC cannot be"
                                 " terminated, so either wait the graceful"
                                 " period of %ld s for its native termination"
                                 " if it vitally depends on some other daemons"
                                 " going down in a controlled way already,"
                                 " or locate and kill the correct %s process"
                                 " on your own; set PCMK_fail_fast=1 to avoid"
                                 " this altogether next time around",
                                 child->name, (long) SHUTDOWN_ESCALATION_PERIOD,
                                 child->command);
                    }
                    next_log = now + 30;
                    child->respawn = FALSE;
                    stop_child(child, SIGTERM);
                    if (phase < pcmk_children[PCMK_CHILD_CONTROLD].start_seq) {
                        g_timeout_add(SHUTDOWN_ESCALATION_PERIOD,
                                      escalate_shutdown, child);
                    }

                } else if (now >= next_log) {
                    next_log = now + 30;
                    crm_notice("Still waiting for %s to terminate "
                               CRM_XS " pid=%lld seq=%d",
                               child->name, (long long) child->pid,
                               child->start_seq);
                }
                return TRUE;
            }

            /* cleanup */
            crm_debug("%s confirmed stopped", child->name);
            child->pid = 0;
        }
    }

    /* send_cluster_id(); */
    crm_notice("Shutdown complete");
    pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_SHUTDOWNCOMPLETE;
    if (!fatal_error && running_with_sbd &&
        !shutdown_complete_state_reported_client_closed) {
        return TRUE;
    }

    {
        const char *delay = pcmk__env_option("shutdown_delay");
        if(delay) {
            sync();
            sleep(crm_get_msec(delay) / 1000);
        }
    }

    g_main_loop_quit(mainloop);

    if (fatal_error) {
        crm_notice("Shutting down and staying down after fatal error");
        pcmk_exit_with_cluster(CRM_EX_FATAL);
    }

    return TRUE;
}

static void
pcmk_ignore(int nsig)
{
    crm_info("Ignoring signal %s (%d)", strsignal(nsig), nsig);
}

static void
pcmk_sigquit(int nsig)
{
    pcmk_panic(__FUNCTION__);
}

void
pcmk_shutdown(int nsig)
{
    if (shutdown_trigger == NULL) {
        shutdown_trigger = mainloop_add_trigger(G_PRIORITY_HIGH, pcmk_shutdown_worker, NULL);
    }
    mainloop_set_trigger(shutdown_trigger);
}

static int32_t
pcmk_ipc_accept(qb_ipcs_connection_t * c, uid_t uid, gid_t gid)
{
    crm_trace("Connection %p", c);
    if (pcmk__new_client(c, uid, gid) == NULL) {
        return -EIO;
    }
    return 0;
}

static void
pcmk_handle_ping_request(pcmk__client_t *c, xmlNode *msg, uint32_t id)
{
    const char *value = NULL;
    xmlNode *ping = NULL;
    xmlNode *reply = NULL;
    time_t pinged = time(NULL);
    const char *from = crm_element_value(msg, F_CRM_SYS_FROM);

    /* Pinged for status */
    crm_trace("Pinged from %s.%s",
              crm_element_value(msg, F_CRM_ORIGIN),
              from?from:"unknown");
    ping = create_xml_node(NULL, XML_CRM_TAG_PING);
    value = crm_element_value(msg, F_CRM_SYS_TO);
    crm_xml_add(ping, XML_PING_ATTR_SYSFROM, value);
    crm_xml_add(ping, XML_PING_ATTR_PACEMAKERDSTATE, pacemakerd_state);
    crm_xml_add_ll(ping, XML_ATTR_TSTAMP, (long long) pinged);
    crm_xml_add(ping, XML_PING_ATTR_STATUS, "ok");
    reply = create_reply(msg, ping);
    free_xml(ping);
    if (reply) {
        if (pcmk__ipc_send_xml(c, id, reply, crm_ipc_server_event) <= 0) {
            crm_err("Failed sending ping-reply");
        }
        free_xml(reply);
    } else {
        crm_err("Failed building ping-reply");
    }
    /* just proceed state on sbd pinging us */
    if (from && strstr(from, "sbd")) {
        if (crm_str_eq(pacemakerd_state,
                       XML_PING_ATTR_PACEMAKERDSTATE_SHUTDOWNCOMPLETE,
                       TRUE)) {
            shutdown_complete_state_reported_to = c->pid;
        } else if (crm_str_eq(pacemakerd_state,
                              XML_PING_ATTR_PACEMAKERDSTATE_WAITPING,
                              TRUE)) {
            pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_STARTINGDAEMONS;
            mainloop_set_trigger(startup_trigger);
        }
    }
}

/* Exit code means? */
static int32_t
pcmk_ipc_dispatch(qb_ipcs_connection_t * qbc, void *data, size_t size)
{
    uint32_t id = 0;
    uint32_t flags = 0;
    const char *task = NULL;
    pcmk__client_t *c = pcmk__find_client(qbc);
    xmlNode *msg = pcmk__client_data2xml(c, data, &id, &flags);

    pcmk__ipc_send_ack(c, id, flags, "ack");
    if (msg == NULL) {
        return 0;
    }

    if (crm_str_eq(task, CRM_OP_PING, TRUE)) {
        pcmk_handle_ping_request(c, msg, id);
    } else {

        if (crm_str_eq(task, CRM_OP_QUIT, TRUE)) {
            /* Time to quit */
            crm_notice("Shutting down in response to ticket %s (%s)",
                       crm_element_value(msg, F_CRM_REFERENCE),
                       crm_element_value(msg, F_CRM_ORIGIN));
            pcmk_shutdown(15);

        } else if (crm_str_eq(task, CRM_OP_RM_NODE_CACHE, TRUE)) {
            /* Send to everyone */
            struct iovec *iov;
            int id = 0;
            const char *name = NULL;

            crm_element_value_int(msg, XML_ATTR_ID, &id);
            name = crm_element_value(msg, XML_ATTR_UNAME);
            crm_notice("Instructing peers to remove references to node %s/%u", name, id);

            iov = calloc(1, sizeof(struct iovec));
            iov->iov_base = dump_xml_unformatted(msg);
            iov->iov_len = 1 + strlen(iov->iov_base);
            send_cpg_iov(iov);

        } else {
            update_process_clients(c);
        }
    }

    free_xml(msg);
    return 0;
}

/* Error code means? */
static int32_t
pcmk_ipc_closed(qb_ipcs_connection_t * c)
{
    pcmk__client_t *client = pcmk__find_client(c);

    if (client == NULL) {
        return 0;
    }
    crm_trace("Connection %p", c);
    pcmk__free_client(client);
    return 0;
}

static void
pcmk_ipc_destroy(qb_ipcs_connection_t * c)
{
    crm_trace("Connection %p", c);
    pcmk_ipc_closed(c);
}

struct qb_ipcs_service_handlers mcp_ipc_callbacks = {
    .connection_accept = pcmk_ipc_accept,
    .connection_created = NULL,
    .msg_process = pcmk_ipc_dispatch,
    .connection_closed = pcmk_ipc_closed,
    .connection_destroyed = pcmk_ipc_destroy
};

static void
send_xml_to_client(gpointer key, gpointer value, gpointer user_data)
{
    pcmk__ipc_send_xml((pcmk__client_t *) value, 0, (xmlNode *) user_data,
                       crm_ipc_server_event);
}

/*!
 * \internal
 * \brief Send an XML message with process list of all known peers to client(s)
 *
 * \param[in] client  Send message to this client, or all clients if NULL
 */
void
update_process_clients(pcmk__client_t *client)
{
    GHashTableIter iter;
    crm_node_t *node = NULL;
    xmlNode *update = create_xml_node(NULL, "nodes");

    if (is_corosync_cluster()) {
        crm_xml_add_int(update, "quorate", pcmk_quorate);
    }

    g_hash_table_iter_init(&iter, crm_peer_cache);
    while (g_hash_table_iter_next(&iter, NULL, (gpointer *) & node)) {
        xmlNode *xml = create_xml_node(update, "node");

        crm_xml_add_int(xml, "id", node->id);
        crm_xml_add(xml, "uname", node->uname);
        crm_xml_add(xml, "state", node->state);
        crm_xml_add_int(xml, "processes", node->processes);
    }

    if(client) {
        crm_trace("Sending process list to client %s", client->id);
        send_xml_to_client(NULL, client, update);

    } else {
        crm_trace("Sending process list to %d clients",
                  pcmk__ipc_client_count());
        pcmk__foreach_ipc_client(send_xml_to_client, update);
    }
    free_xml(update);
}

/*!
 * \internal
 * \brief Send a CPG message with local node's process list to all peers
 */
static void
update_process_peers(void)
{
    /* Do nothing for corosync-2 based clusters */

    struct iovec *iov = calloc(1, sizeof(struct iovec));

    CRM_ASSERT(iov);
    if (local_name) {
        iov->iov_base = crm_strdup_printf("<node uname=\"%s\" proclist=\"%u\"/>",
                                          local_name, get_process_list());
    } else {
        iov->iov_base = crm_strdup_printf("<node proclist=\"%u\"/>",
                                          get_process_list());
    }
    iov->iov_len = strlen(iov->iov_base) + 1;
    crm_trace("Sending %s", (char*) iov->iov_base);
    send_cpg_iov(iov);
}

/*!
 * \internal
 * \brief Update a node's process list, notifying clients and peers if needed
 *
 * \param[in] id     Node ID of affected node
 * \param[in] uname  Uname of affected node
 * \param[in] procs  Affected node's process list mask
 *
 * \return TRUE if the process list changed, FALSE otherwise
 */
static gboolean
update_node_processes(uint32_t id, const char *uname, uint32_t procs)
{
    gboolean changed = FALSE;
    crm_node_t *node = crm_get_peer(id, uname);

    if (procs != 0) {
        if (procs != node->processes) {
            crm_debug("Node %s now has process list: %.32x (was %.32x)",
                      node->uname, procs, node->processes);
            node->processes = procs;
            changed = TRUE;

            /* If local node's processes have changed, notify clients/peers */
            if (id == local_nodeid) {
                update_process_clients(NULL);
                update_process_peers();
            }

        } else {
            crm_trace("Node %s still has process list: %.32x", node->uname, procs);
        }
    }
    return changed;
}


static pcmk__cli_option_t long_options[] = {
    // long option, argument type, storage, short option, description, flags
    {
        "help", no_argument, NULL, '?',
        "\tThis text", pcmk__option_default
    },
    {
        "version", no_argument, NULL, '$',
        "\tVersion information", pcmk__option_default
    },
    {
        "verbose", no_argument, NULL, 'V',
        "\tIncrease debug output", pcmk__option_default
    },
    {
        "shutdown", no_argument, NULL, 'S',
        "\tInstruct Pacemaker to shutdown on this machine", pcmk__option_default
    },
    {
        "features", no_argument, NULL, 'F',
        "\tDisplay full version and list of features Pacemaker was built with",
        pcmk__option_default
    },
    {
        "-spacer-", no_argument, NULL, '-',
        "\nAdditional Options:", pcmk__option_default
    },
    {
        "foreground", no_argument, NULL, 'f',
        "\t(Ignored) Pacemaker always runs in the foreground",
        pcmk__option_default
    },
    {
        "pid-file", required_argument, NULL, 'p',
        "\t(Ignored) Daemon pid file location", pcmk__option_default
    },
    {
        "standby", no_argument, NULL, 's',
        "\tStart node in standby state", pcmk__option_default
    },
    { 0, 0, 0, 0 }
};

static void
mcp_chown(const char *path, uid_t uid, gid_t gid)
{
    int rc = chown(path, uid, gid);

    if (rc < 0) {
        crm_warn("Cannot change the ownership of %s to user %s and gid %d: %s",
                 path, CRM_DAEMON_USER, gid, pcmk_strerror(errno));
    }
}

/*!
 * \internal
 * \brief Check the liveness of the child based on IPC name and PID if tracked
 *
 * \param[inout] child  Child tracked data
 *
 * \return Standard Pacemaker return code
 *
 * \note Return codes of particular interest include pcmk_rc_ipc_unresponsive
 *       indicating that no trace of IPC liveness was detected,
 *       pcmk_rc_ipc_unauthorized indicating that the IPC endpoint is blocked by
 *       an unauthorized process, and pcmk_rc_ipc_pid_only indicating that
 *       the child is up by PID but not IPC end-point (possibly starting).
 * \note This function doesn't modify any of \p child members but \c pid,
 *       and is not actively toying with processes as such but invoking
 *       \c stop_child in one particular case (there's for some reason
 *       a different authentic holder of the IPC end-point).
 */
static int
child_liveness(pcmk_child_t *child)
{
    uid_t cl_uid = 0;
    gid_t cl_gid = 0;
    const uid_t root_uid = 0;
    const gid_t root_gid = 0;
    const uid_t *ref_uid;
    const gid_t *ref_gid;
    int rc = pcmk_rc_ipc_unresponsive;
    pid_t ipc_pid = 0;

    if (child->endpoint == NULL
            && (child->pid <= 0 || child->pid == PCMK__SPECIAL_PID)) {
        crm_err("Cannot track child %s for missing both API end-point and PID",
                child->name);
        rc = EINVAL; // Misuse of function when child is not trackable

    } else if (child->endpoint != NULL) {
        int legacy_rc = pcmk_ok;

        if (child->uid == NULL) {
            ref_uid = &root_uid;
            ref_gid = &root_gid;
        } else {
            ref_uid = &cl_uid;
            ref_gid = &cl_gid;
            legacy_rc = pcmk_daemon_user(&cl_uid, &cl_gid);
        }

        if (legacy_rc < 0) {
            rc = pcmk_legacy2rc(legacy_rc);
            crm_err("Could not find user and group IDs for user %s: %s "
                    CRM_XS " rc=%d", CRM_DAEMON_USER, pcmk_rc_str(rc), rc);
        } else {
            rc = pcmk__ipc_is_authentic_process_active(child->endpoint,
                                                       *ref_uid, *ref_gid,
                                                       &ipc_pid);
            if ((rc == pcmk_rc_ok) || (rc == pcmk_rc_ipc_unresponsive)) {
                if (child->pid <= 0) {
                    /* If rc is pcmk_rc_ok, ipc_pid is nonzero and this
                     * initializes a new child. If rc is
                     * pcmk_rc_ipc_unresponsive, ipc_pid is zero, and we will
                     * investigate further.
                     */
                    child->pid = ipc_pid;
                } else if ((ipc_pid != 0) && (child->pid != ipc_pid)) {
                    /* An unexpected (but authorized) process is responding to
                     * IPC. Investigate further.
                     */
                    rc = pcmk_rc_ipc_unresponsive;
                }
            }
        }
    }

    if (rc == pcmk_rc_ipc_unresponsive) {
        /* If we get here, a child without IPC is being tracked, no IPC liveness
         * has been detected, or IPC liveness has been detected with an
         * unexpected (but authorized) process. This is safe on FreeBSD since
         * the only change possible from a proper child's PID into "special" PID
         * of 1 behind more loosely related process.
         */
        int ret = pcmk__pid_active(child->pid, child->name);

        if (ipc_pid && ((ret != pcmk_rc_ok)
                        || ipc_pid == PCMK__SPECIAL_PID
                        || (pcmk__pid_active(ipc_pid,
                                             child->name) == pcmk_rc_ok))) {
            /* An unexpected (but authorized) process was detected at the IPC
             * endpoint, and either it is active, or the child we're tracking is
             * not.
             */

            if (ret == pcmk_rc_ok) {
                /* The child we're tracking is active. Kill it, and adopt the
                 * detected process. This assumes that our children don't fork
                 * (thus getting a different PID owning the IPC), but rather the
                 * tracking got out of sync because of some means external to
                 * Pacemaker, and adopting the detected process is better than
                 * killing it and possibly having to spawn a new child.
                 */
                /* not possessing IPC, afterall (what about corosync CPG?) */
                stop_child(child, SIGKILL);
            }
            rc = pcmk_rc_ok;
            child->pid = ipc_pid;
        } else if (ret == pcmk_rc_ok) {
            // Our tracked child's PID was found active, but not its IPC
            rc = pcmk_rc_ipc_pid_only;
        } else if ((child->pid == 0) && (ret == EINVAL)) {
            // FreeBSD can return EINVAL
            rc = pcmk_rc_ipc_unresponsive;
        } else {
            switch (ret) {
                case EACCES:
                    rc = pcmk_rc_ipc_unauthorized;
                    break;
                case ESRCH:
                    rc = pcmk_rc_ipc_unresponsive;
                    break;
                default:
                    rc = ret;
                    break;
            }
        }
    }
    return rc;
}

static gboolean
check_active_before_startup_processes(gpointer user_data)
{
    int start_seq = 1, lpc = 0;
    static int max = SIZEOF(pcmk_children);
    gboolean keep_tracking = FALSE;

    for (start_seq = 1; start_seq < max; start_seq++) {
        for (lpc = 0; lpc < max; lpc++) {
            if (pcmk_children[lpc].active_before_startup == FALSE) {
                /* we are already tracking it as a child process. */
                continue;
            } else if (start_seq != pcmk_children[lpc].start_seq) {
                continue;
            } else {
                int rc = child_liveness(&pcmk_children[lpc]);

                switch (rc) {
                    case pcmk_rc_ok:
                        break;
                    case pcmk_rc_ipc_unresponsive:
                    case pcmk_rc_ipc_pid_only: // This case: it was previously OK
                        if (pcmk_children[lpc].respawn == TRUE) {
                            crm_err("%s[%lld] terminated%s", pcmk_children[lpc].name,
                                    (long long) PCMK__SPECIAL_PID_AS_0(pcmk_children[lpc].pid),
                                    (rc == pcmk_rc_ipc_pid_only)? " as IPC server" : "");
                        } else {
                            /* orderly shutdown */
                            crm_notice("%s[%lld] terminated%s", pcmk_children[lpc].name,
                                       (long long) PCMK__SPECIAL_PID_AS_0(pcmk_children[lpc].pid),
                                       (rc == pcmk_rc_ipc_pid_only)? " as IPC server" : "");
                        }
                        pcmk_process_exit(&(pcmk_children[lpc]));
                        continue;
                    default:
                        crm_exit(CRM_EX_FATAL);
                        break;  /* static analysis/noreturn */
                }
            }
            /* at least one of the processes found at startup
             * is still going, so keep this recurring timer around */
            keep_tracking = TRUE;
        }
    }

    global_keep_tracking = keep_tracking;
    return keep_tracking;
}

/*!
 * \internal
 * \brief Initial one-off check of the pre-existing "child" processes
 *
 * With "child" process, we mean the subdaemon that defines an API end-point
 * (all of them do as of the comment) -- the possible complement is skipped
 * as it is deemed it has no such shared resources to cause conflicts about,
 * hence it can presumably be started anew without hesitation.
 * If that won't hold true in the future, the concept of a shared resource
 * will have to be generalized beyond the API end-point.
 *
 * For boundary cases that the "child" is still starting (IPC end-point is yet
 * to be witnessed), or more rarely (practically FreeBSD only), when there's
 * a pre-existing "untrackable" authentic process, we give the situation some
 * time to possibly unfold in the right direction, meaning that said socket
 * will appear or the unattainable process will disappear per the observable
 * IPC, respectively.
 *
 * \return Standard Pacemaker return code
 *
 * \note Since this gets run at the very start, \c respawn_count fields
 *       for particular children get temporarily overloaded with "rounds
 *       of waiting" tracking, restored once we are about to finish with
 *       success (i.e. returning value >=0) and will remain unrestored
 *       otherwise.  One way to suppress liveness detection logic for
 *       particular child is to set the said value to a negative number.
 */
#define WAIT_TRIES 4  /* together with interleaved sleeps, worst case ~ 1s */
static int
find_and_track_existing_processes(void)
{
    bool tracking = false;
    bool wait_in_progress;
    int rc;
    size_t i, rounds;

    for (rounds = 1; rounds <= WAIT_TRIES; rounds++) {
        wait_in_progress = false;
        for (i = 0; i < SIZEOF(pcmk_children); i++) {

            if ((pcmk_children[i].endpoint == NULL)
                || (pcmk_children[i].respawn_count < 0)) {
                continue;
            }

            rc = child_liveness(&pcmk_children[i]);
            if (rc == pcmk_rc_ipc_unresponsive) {
                /* As a speculation, don't give up if there are more rounds to
                 * come for other reasons, but don't artificially wait just
                 * because of this, since we would preferably start ASAP.
                 */
                continue;
            }

            pcmk_children[i].respawn_count = rounds;
            switch (rc) {
                case pcmk_rc_ok:
                    if (pcmk_children[i].pid == PCMK__SPECIAL_PID) {
                        if (crm_is_true(getenv("PCMK_fail_fast"))) {
                            crm_crit("Cannot reliably track pre-existing"
                                     " authentic process behind %s IPC on this"
                                     " platform and PCMK_fail_fast requested",
                                     pcmk_children[i].endpoint);
                            return EOPNOTSUPP;
                        } else if (pcmk_children[i].respawn_count == WAIT_TRIES) {
                            crm_notice("Assuming pre-existing authentic, though"
                                       " on this platform untrackable, process"
                                       " behind %s IPC is stable (was in %d"
                                       " previous samples) so rather than"
                                       " bailing out (PCMK_fail_fast not"
                                       " requested), we just switch to a less"
                                       " optimal IPC liveness monitoring"
                                       " (not very suitable for heavy load)",
                                       pcmk_children[i].name, WAIT_TRIES - 1);
                            crm_warn("The process behind %s IPC cannot be"
                                     " terminated, so the overall shutdown"
                                     " will get delayed implicitly (%ld s),"
                                     " which serves as a graceful period for"
                                     " its native termination if it vitally"
                                     " depends on some other daemons going"
                                     " down in a controlled way already",
                                     pcmk_children[i].name,
                                     (long) SHUTDOWN_ESCALATION_PERIOD);
                        } else {
                            wait_in_progress = true;
                            crm_warn("Cannot reliably track pre-existing"
                                     " authentic process behind %s IPC on this"
                                     " platform, can still disappear in %d"
                                     " attempt(s)", pcmk_children[i].endpoint,
                                     WAIT_TRIES - pcmk_children[i].respawn_count);
                            continue;
                        }
                    }
                    crm_notice("Tracking existing %s process (pid=%lld)",
                               pcmk_children[i].name,
                               (long long) PCMK__SPECIAL_PID_AS_0(
                                               pcmk_children[i].pid));
                    pcmk_children[i].respawn_count = -1;  /* 0~keep watching */
                    pcmk_children[i].active_before_startup = TRUE;
                    tracking = true;
                    break;
                case pcmk_rc_ipc_pid_only:
                    if (pcmk_children[i].respawn_count == WAIT_TRIES) {
                        crm_crit("%s IPC end-point for existing authentic"
                                 " process %lld did not (re)appear",
                                 pcmk_children[i].endpoint,
                                 (long long) PCMK__SPECIAL_PID_AS_0(
                                                 pcmk_children[i].pid));
                        return rc;
                    }
                    wait_in_progress = true;
                    crm_warn("Cannot find %s IPC end-point for existing"
                             " authentic process %lld, can still (re)appear"
                             " in %d attempts (?)",
                             pcmk_children[i].endpoint,
                             (long long) PCMK__SPECIAL_PID_AS_0(
                                             pcmk_children[i].pid),
                             WAIT_TRIES - pcmk_children[i].respawn_count);
                    continue;
                default:
                    crm_crit("Checked liveness of %s: %s " CRM_XS " rc=%d",
                             pcmk_children[i].name, pcmk_rc_str(rc), rc);
                    return rc;
            }
        }
        if (!wait_in_progress) {
            break;
        }
        (void) poll(NULL, 0, 250);  /* a bit for changes to possibly happen */
    }
    for (i = 0; i < SIZEOF(pcmk_children); i++) {
        pcmk_children[i].respawn_count = 0;  /* restore pristine state */
    }

    if (tracking) {
        g_timeout_add_seconds(PCMK_PROCESS_CHECK_INTERVAL,
                              check_active_before_startup_processes, NULL);
    }
    return pcmk_rc_ok;
}

static gboolean
init_children_processes(void *user_data)
{
    int start_seq = 1, lpc = 0;
    static int max = SIZEOF(pcmk_children);

    /* start any children that have not been detected */
    for (start_seq = 1; start_seq < max; start_seq++) {
        /* don't start anything with start_seq < 1 */
        for (lpc = 0; lpc < max; lpc++) {
            if (pcmk_children[lpc].pid != 0) {
                /* we are already tracking it */
                continue;
            }

            if (start_seq == pcmk_children[lpc].start_seq) {
                start_child(&(pcmk_children[lpc]));
            }
        }
    }

    /* From this point on, any daemons being started will be due to
     * respawning rather than node start.
     *
     * This may be useful for the daemons to know
     */
    setenv("PCMK_respawned", "true", 1);
    pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_RUNNING;
    return TRUE;
}

static void
mcp_cpg_destroy(gpointer user_data)
{
    crm_crit("Lost connection to cluster layer, shutting down");
    crm_exit(CRM_EX_DISCONNECT);
}

/*!
 * \internal
 * \brief Process a CPG message (process list or manual peer cache removal)
 *
 * \param[in] handle     CPG connection (ignored)
 * \param[in] groupName  CPG group name (ignored)
 * \param[in] nodeid     ID of affected node
 * \param[in] pid        Process ID (ignored)
 * \param[in] msg        CPG XML message
 * \param[in] msg_len    Length of msg in bytes (ignored)
 */
static void
mcp_cpg_deliver(cpg_handle_t handle,
                 const struct cpg_name *groupName,
                 uint32_t nodeid, uint32_t pid, void *msg, size_t msg_len)
{
    xmlNode *xml = string2xml(msg);
    const char *task = crm_element_value(xml, F_CRM_TASK);

    crm_trace("Received CPG message (%s): %.200s",
              (task? task : "process list"), (char*)msg);

    if (task == NULL) {
        if (nodeid == local_nodeid) {
            crm_debug("Ignoring message with local node's process list");
        } else {
            uint32_t procs = 0;
            const char *uname = crm_element_value(xml, "uname");

            crm_element_value_int(xml, "proclist", (int *)&procs);
            if (update_node_processes(nodeid, uname, procs)) {
                update_process_clients(NULL);
            }
        }

    } else if (crm_str_eq(task, CRM_OP_RM_NODE_CACHE, TRUE)) {
        int id = 0;
        const char *name = NULL;

        crm_element_value_int(xml, XML_ATTR_ID, &id);
        name = crm_element_value(xml, XML_ATTR_UNAME);
        reap_crm_member(id, name);
    }

    if (xml != NULL) {
        free_xml(xml);
    }
}

static void
mcp_cpg_membership(cpg_handle_t handle,
                    const struct cpg_name *groupName,
                    const struct cpg_address *member_list, size_t member_list_entries,
                    const struct cpg_address *left_list, size_t left_list_entries,
                    const struct cpg_address *joined_list, size_t joined_list_entries)
{
    /* Update peer cache if needed */
    pcmk_cpg_membership(handle, groupName, member_list, member_list_entries,
                        left_list, left_list_entries,
                        joined_list, joined_list_entries);

    /* Always broadcast our own presence after any membership change */
    update_process_peers();
}

static gboolean
mcp_quorum_callback(unsigned long long seq, gboolean quorate)
{
    pcmk_quorate = quorate;
    return TRUE;
}

static void
mcp_quorum_destroy(gpointer user_data)
{
    crm_info("connection lost");
}

int
main(int argc, char **argv)
{
    int rc;
    int flag;
    int argerr = 0;

    int option_index = 0;
    gboolean shutdown = FALSE;

    uid_t pcmk_uid = 0;
    gid_t pcmk_gid = 0;
    struct rlimit cores;
    crm_ipc_t *old_instance = NULL;
    qb_ipcs_service_t *ipcs = NULL;
    static crm_cluster_t cluster;

    crm_log_preinit(NULL, argc, argv);
    pcmk__set_cli_options(NULL, "[options]", long_options,
                          "primary Pacemaker daemon that launches and "
                          "monitors all subsidiary Pacemaker daemons");
    mainloop_add_signal(SIGHUP, pcmk_ignore);
    mainloop_add_signal(SIGQUIT, pcmk_sigquit);

    while (1) {
        flag = pcmk__next_cli_option(argc, argv, &option_index, NULL);
        if (flag == -1)
            break;

        switch (flag) {
            case 'V':
                crm_bump_log_level(argc, argv);
                break;
            case 'f':
                /* Legacy */
                break;
            case 'p':
                pid_file = optarg;
                break;
            case 's':
                pcmk__set_env_option("node_start_state", "standby");
                break;
            case '$':
            case '?':
                pcmk__cli_help(flag, CRM_EX_OK);
                break;
            case 'S':
                shutdown = TRUE;
                break;
            case 'F':
                printf("Pacemaker %s (Build: %s)\n Supporting v%s: %s\n", PACEMAKER_VERSION, BUILD_VERSION,
                       CRM_FEATURE_SET, CRM_FEATURES);
                crm_exit(CRM_EX_OK);
            default:
                printf("Argument code 0%o (%c) is not (?yet?) supported\n", flag, flag);
                ++argerr;
                break;
        }
    }

    if (optind < argc) {
        printf("non-option ARGV-elements: ");
        while (optind < argc)
            printf("%s ", argv[optind++]);
        printf("\n");
    }
    if (argerr) {
        pcmk__cli_help('?', CRM_EX_USAGE);
    }


    setenv("LC_ALL", "C", 1);

    pcmk__set_env_option("mcp", "true");

    crm_log_init(NULL, LOG_INFO, TRUE, FALSE, argc, argv, FALSE);

    crm_debug("Checking for existing Pacemaker instance");
    old_instance = crm_ipc_new(CRM_SYSTEM_MCP, 0);
    (void) crm_ipc_connect(old_instance);

    if (shutdown) {
        crm_debug("Shutting down existing Pacemaker instance by request");
        while (crm_ipc_connected(old_instance)) {
            xmlNode *cmd =
                create_request(CRM_OP_QUIT, NULL, NULL, CRM_SYSTEM_MCP, CRM_SYSTEM_MCP, NULL);

            crm_debug(".");
            crm_ipc_send(old_instance, cmd, 0, 0, NULL);
            free_xml(cmd);

            sleep(2);
        }
        crm_ipc_close(old_instance);
        crm_ipc_destroy(old_instance);
        crm_exit(CRM_EX_OK);

    } else if (crm_ipc_connected(old_instance)) {
        crm_ipc_close(old_instance);
        crm_ipc_destroy(old_instance);
        crm_err("Aborting start-up because active Pacemaker instance found");
        crm_exit(CRM_EX_FATAL);
    }

    crm_ipc_close(old_instance);
    crm_ipc_destroy(old_instance);

    if (mcp_read_config() == FALSE) {
        crm_notice("Could not obtain corosync config data, exiting");
        crm_exit(CRM_EX_UNAVAILABLE);
    }

    // OCF shell functions and cluster-glue need facility under different name
    {
        const char *facility = pcmk__env_option("logfacility");

        if (facility && safe_str_neq(facility, "none")) {
            setenv("HA_LOGFACILITY", facility, 1);
        }
    }

    crm_notice("Starting Pacemaker %s "CRM_XS" build=%s features:%s",
               PACEMAKER_VERSION, BUILD_VERSION, CRM_FEATURES);
    mainloop = g_main_loop_new(NULL, FALSE);

    rc = getrlimit(RLIMIT_CORE, &cores);
    if (rc < 0) {
        crm_perror(LOG_ERR, "Cannot determine current maximum core size.");
    } else {
        if (cores.rlim_max == 0 && geteuid() == 0) {
            cores.rlim_max = RLIM_INFINITY;
        } else {
            crm_info("Maximum core file size is: %lu", (unsigned long)cores.rlim_max);
        }
        cores.rlim_cur = cores.rlim_max;

        rc = setrlimit(RLIMIT_CORE, &cores);
        if (rc < 0) {
            crm_perror(LOG_ERR,
                       "Core file generation will remain disabled."
                       " Core files are an important diagnostic tool, so"
                       " please consider enabling them by default.");
        }
    }

    if (pcmk_daemon_user(&pcmk_uid, &pcmk_gid) < 0) {
        crm_err("Cluster user %s does not exist, aborting Pacemaker startup", CRM_DAEMON_USER);
        crm_exit(CRM_EX_NOUSER);
    }

    // Used by some resource agents
    if ((mkdir(CRM_STATE_DIR, 0750) < 0) && (errno != EEXIST)) {
        crm_warn("Could not create " CRM_STATE_DIR ": %s", pcmk_strerror(errno));
    } else {
        mcp_chown(CRM_STATE_DIR, pcmk_uid, pcmk_gid);
    }

    /* Used to store core/blackbox/scheduler/cib files in */
    crm_build_path(CRM_PACEMAKER_DIR, 0750);
    mcp_chown(CRM_PACEMAKER_DIR, pcmk_uid, pcmk_gid);

    /* Used to store core files in */
    crm_build_path(CRM_CORE_DIR, 0750);
    mcp_chown(CRM_CORE_DIR, pcmk_uid, pcmk_gid);

    /* Used to store blackbox dumps in */
    crm_build_path(CRM_BLACKBOX_DIR, 0750);
    mcp_chown(CRM_BLACKBOX_DIR, pcmk_uid, pcmk_gid);

    // Used to store scheduler inputs in
    crm_build_path(PE_STATE_DIR, 0750);
    mcp_chown(PE_STATE_DIR, pcmk_uid, pcmk_gid);

    /* Used to store the cluster configuration */
    crm_build_path(CRM_CONFIG_DIR, 0750);
    mcp_chown(CRM_CONFIG_DIR, pcmk_uid, pcmk_gid);

    // Don't build CRM_RSCTMP_DIR, pacemaker-execd will do it

    ipcs = mainloop_add_ipc_server(CRM_SYSTEM_MCP, QB_IPC_NATIVE, &mcp_ipc_callbacks);
    if (ipcs == NULL) {
        crm_err("Couldn't start IPC server");
        crm_exit(CRM_EX_OSERR);
    }

    /* Allows us to block shutdown */
    if (cluster_connect_cfg(&local_nodeid) == FALSE) {
        crm_err("Couldn't connect to Corosync's CFG service");
        crm_exit(CRM_EX_PROTOCOL);
    }

    if(pcmk_locate_sbd() > 0) {
        setenv("PCMK_watchdog", "true", 1);
        running_with_sbd = TRUE;
    } else {
        setenv("PCMK_watchdog", "false", 1);
    }

    switch (find_and_track_existing_processes()) {
        case pcmk_rc_ok:
            break;
        case pcmk_rc_ipc_unauthorized:
            crm_exit(CRM_EX_CANTCREAT);
        default:
            crm_exit(CRM_EX_FATAL);
    };

    cluster.destroy = mcp_cpg_destroy;
    cluster.cpg.cpg_deliver_fn = mcp_cpg_deliver;
    cluster.cpg.cpg_confchg_fn = mcp_cpg_membership;

    crm_set_autoreap(FALSE);

    rc = pcmk_ok;

    if (cluster_connect_cpg(&cluster) == FALSE) {
        crm_err("Couldn't connect to Corosync's CPG service");
        rc = -ENOPROTOOPT;

    } else if (cluster_connect_quorum(mcp_quorum_callback, mcp_quorum_destroy)
               == FALSE) {
        rc = -ENOTCONN;

    } else {
        local_name = get_local_node_name();
        update_node_processes(local_nodeid, local_name, get_process_list());

        mainloop_add_signal(SIGTERM, pcmk_shutdown);
        mainloop_add_signal(SIGINT, pcmk_shutdown);

        if (running_with_sbd) {
            pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_WAITPING;
            startup_trigger = mainloop_add_trigger(G_PRIORITY_HIGH, init_children_processes, NULL);
        } else {
            pacemakerd_state = XML_PING_ATTR_PACEMAKERDSTATE_STARTINGDAEMONS;
            init_children_processes(NULL);
        }

        crm_notice("Pacemaker daemon successfully started and accepting connections");
        g_main_loop_run(mainloop);
    }

    if (ipcs) {
        crm_trace("Closing IPC server");
        mainloop_del_ipc_server(ipcs);
        ipcs = NULL;
    }

    g_main_loop_unref(mainloop);

    cluster_disconnect_cpg(&cluster);
    cluster_disconnect_cfg();

    crm_exit(crm_errno2exit(rc));
}
