/*
 * Copyright 2020 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#include <crm_internal.h>

#include <stdlib.h>
#include <time.h>

#include <crm/crm.h>
#include <crm/msg_xml.h>
#include <crm/common/xml.h>
#include <crm/common/ipc.h>
#include <crm/common/ipc_internal.h>
#include <crm/common/ipc_pacemakerd.h>
#include "crmcommon_private.h"

typedef struct pacemakerd_api_private_s {
    enum pcmk_pacemakerd_state state;
    char *client_uuid;
    unsigned int replies_expected;
} pacemakerd_api_private_t;

static const char *pacemakerd_state_str[] = {
    XML_PING_ATTR_PACEMAKERDSTATE_INIT,
    XML_PING_ATTR_PACEMAKERDSTATE_STARTINGDAEMONS,
    XML_PING_ATTR_PACEMAKERDSTATE_WAITPING,
    XML_PING_ATTR_PACEMAKERDSTATE_RUNNING,
    XML_PING_ATTR_PACEMAKERDSTATE_SHUTTINGDOWN,
    XML_PING_ATTR_PACEMAKERDSTATE_SHUTDOWNCOMPLETE
};

enum pcmk_pacemakerd_state
pcmk_pacemakerd_api_pacemakerd_state_text2enum(const char *state)
{
    int i;

    if (state == NULL) {
        return pcmk_pacemakerd_state_invalid;
    }
    for (i=pcmk_pacemakerd_state_init; i <= pcmk_pacemakerd_state_max;
         i++) {
        if (crm_str_eq(state, pacemakerd_state_str[i], TRUE)) {
            return i;
        }
    }
    return pcmk_pacemakerd_state_invalid;
}

const char *
pcmk_pacemakerd_api_pacemakerd_state_enum2text(
    enum pcmk_pacemakerd_state state)
{
    if ((state >= pcmk_pacemakerd_state_init) &&
        (state <= pcmk_pacemakerd_state_max)) {
        return pacemakerd_state_str[state];
    }
    return NULL;
}

// \return Standard Pacemaker return code
static int
new_data(pcmk_ipc_api_t *api)
{
    struct pacemakerd_api_private_s *private = NULL;

    api->api_data = calloc(1, sizeof(struct pacemakerd_api_private_s));

    if (api->api_data == NULL) {
        return errno;
    }

    private = api->api_data;
    private->state = pcmk_pacemakerd_state_invalid;
    /* other as with cib or controld pacemakerd is just addressed
       from the local node - pid is a ID
     */
    private->client_uuid = pcmk__getpid_s();

    return pcmk_rc_ok;
}

static void
free_data(void *data)
{
    free(data);
}

// \return Standard Pacemaker return code
static int
post_connect(pcmk_ipc_api_t *api)
{
    struct pacemakerd_api_private_s *private = NULL;

    if (api->api_data == NULL) {
        return pcmk_rc_no_input;
    }
    private = api->api_data;
    private->state = pcmk_pacemakerd_state_invalid;

    return pcmk_rc_ok;
}

static void
post_disconnect(pcmk_ipc_api_t *api)
{
    struct pacemakerd_api_private_s *private = NULL;

    if (api->api_data == NULL) {
        return;
    }
    private = api->api_data;
    private->state = pcmk_pacemakerd_state_invalid;

    return;
}

static bool
reply_expected(pcmk_ipc_api_t *api, xmlNode *request)
{
    const char *command = crm_element_value(request, F_CRM_TASK);

    if (command == NULL) {
        return false;
    }

    // We only need to handle commands that functions in this file can send
    return !strcmp(command, CRM_OP_PING);
}

static void
dispatch(pcmk_ipc_api_t *api, xmlNode *reply)
{
    struct pacemakerd_api_private_s *private = api->api_data;
    crm_exit_t status = CRM_EX_OK;
    const char *value = NULL;
    pcmk_pacemakerd_api_reply_t reply_data = {
        pcmk_pacemakerd_reply_unknown
    };

    if (private->replies_expected > 0) {
        private->replies_expected--;
    }

    value = crm_element_value(reply, F_CRM_MSG_TYPE);
    if ((value == NULL) || (strcmp(value, XML_ATTR_RESPONSE))) {
        crm_debug("Unrecognizable pacemakerd message: invalid message type '%s'",
                  crm_str(value));
        status = CRM_EX_PROTOCOL;
        reply = NULL;
    }

    if (crm_element_value(reply, XML_ATTR_REFERENCE) == NULL) {
        crm_debug("Unrecognizable pacemakerd message: no reference");
        status = CRM_EX_PROTOCOL;
        reply = NULL;
    }

    value = crm_element_value(reply, F_CRM_TASK);
    if ((value == NULL) || strcmp(value, CRM_OP_PING)) {
        crm_debug("Unrecognizable pacemakerd message: '%s'", crm_str(value));
        status = CRM_EX_PROTOCOL;
        reply = NULL;
    }

    // Parse useful info from reply

    if (reply != NULL) {
        xmlNode *msg_data = get_message_xml(reply, F_CRM_DATA);
        const char *status =
            crm_element_value(msg_data, XML_PING_ATTR_STATUS);
        long long value_ll = 0;

        reply_data.reply_type = pcmk_pacemakerd_reply_ping;
        reply_data.data.ping.state =
            pcmk_pacemakerd_api_pacemakerd_state_text2enum(
                crm_element_value(msg_data, XML_PING_ATTR_PACEMAKERDSTATE));
        reply_data.data.ping.status =
            crm_str_eq(status, "ok", FALSE)?pcmk_rc_ok:pcmk_rc_error;

        crm_element_value_ll(msg_data, XML_ATTR_TSTAMP, &value_ll);
        reply_data.data.ping.last_good = (time_t) value_ll;
        reply_data.data.ping.sys_from = crm_element_value(msg_data,
                                            XML_PING_ATTR_SYSFROM);
    }

    pcmk__call_ipc_callback(api, pcmk_ipc_event_reply, status, &reply_data);
}

pcmk__ipc_methods_t *
pcmk__pacemakerd_api_methods()
{
    pcmk__ipc_methods_t *cmds = calloc(1, sizeof(pcmk__ipc_methods_t));

    if (cmds != NULL) {
        cmds->new_data = new_data;
        cmds->free_data = free_data;
        cmds->post_connect = post_connect;
        cmds->reply_expected = reply_expected;
        cmds->dispatch = dispatch;
        cmds->post_disconnect = post_disconnect;
    }
    return cmds;
}

int
pcmk_pacemakerd_api_ping(pcmk_ipc_api_t *api, const char *ipc_name)
{
    pacemakerd_api_private_t *private;
    xmlNode *cmd;
    int rc;

    CRM_CHECK(api != NULL, return -EINVAL);
    private = api->api_data;
    CRM_ASSERT(private != NULL);

    cmd = create_request(CRM_OP_PING, NULL, NULL, CRM_SYSTEM_MCP,
        ipc_name?ipc_name:((crm_system_name? crm_system_name : "client")),
        private->client_uuid);

    if (cmd) {
        rc = pcmk__send_ipc_request(api, cmd);
        if (rc != pcmk_rc_ok) {
            crm_debug("Couldn't ping pacemakerd: %s rc=%d",
                pcmk_strerror(rc), rc);
            rc = -ECOMM;
        } else {
            private->replies_expected++;
        }
        free_xml(cmd);
    } else {
        rc = -ENOMSG;
    }

    return rc;
}

unsigned int
pcmk_pacemakerd_api_replies_expected(pcmk_ipc_api_t *api)
{
    struct pacemakerd_api_private_s *private = api->api_data;

    return private->replies_expected;
}
