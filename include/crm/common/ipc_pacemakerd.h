/*
 * Copyright 2020 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#ifndef PCMK__IPC_PACEMAKERD__H
#  define PCMK__IPC_PACEMAKERD__H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * \file
 * \brief IPC commands for Pacemakerd
 *
 * \ingroup core
 */

#include <stdbool.h>                    // bool
#include <libxml/tree.h>                // xmlNode
#include <crm/common/ipc.h>             // pcmk_ipc_api_t

enum pcmk_pacemakerd_state {
    pcmk_pacemakerd_state_invalid = -1,
    pcmk_pacemakerd_state_init = 0,
    pcmk_pacemakerd_state_starting_daemons,
    pcmk_pacemakerd_state_wait_for_ping,
    pcmk_pacemakerd_state_running,
    pcmk_pacemakerd_state_shutting_down,
    pcmk_pacemakerd_state_shutdown_complete,
    pcmk_pacemakerd_state_max = pcmk_pacemakerd_state_shutdown_complete,
};

//! Possible types of pacemakerd replies
enum pcmk_pacemakerd_api_reply {
    pcmk_pacemakerd_reply_unknown,
    pcmk_pacemakerd_reply_ping,
};

/*!
 * Pacemakerd reply passed to event callback
 */
typedef struct {
    enum pcmk_pacemakerd_api_reply reply_type;

    union {
        // pcmk_pacemakerd_reply_ping
        struct {
            const char *sys_from;
            enum pcmk_pacemakerd_state state;
            time_t last_good;
            int status;
        } ping;
    } data;
} pcmk_pacemakerd_api_reply_t;

int pcmk_pacemakerd_api_ping(pcmk_ipc_api_t *api, const char *ipc_name);
unsigned int
    pcmk_pacemakerd_api_replies_expected(pcmk_ipc_api_t *api);
enum pcmk_pacemakerd_state
    pcmk_pacemakerd_api_pacemakerd_state_text2enum(const char *state);
const char
    *pcmk_pacemakerd_api_pacemakerd_state_enum2text(enum pcmk_pacemakerd_state state);

#ifdef __cplusplus
}
#endif

#endif // PCMK__IPC_PACEMAKERD__H
