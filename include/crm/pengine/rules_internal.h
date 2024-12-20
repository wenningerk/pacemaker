/*
 * Copyright 2015-2024 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#ifndef PCMK__CRM_PENGINE_RULES_INTERNAL__H
#define PCMK__CRM_PENGINE_RULES_INTERNAL__H

#include <glib.h>
#include <libxml/tree.h>

#include <crm/common/iso8601.h>

#ifdef __cplusplus
extern "C" {
#endif

GList *pe_unpack_alerts(const xmlNode *alerts);
void pe_free_alert_list(GList *alert_list);

#ifdef __cplusplus
}
#endif

#endif // PCMK__CRM_PENGINE_RULES_INTERNAL__H
