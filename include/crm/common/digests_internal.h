/*
 * Copyright 2004-2023 the Pacemaker project contributors
 *
 * The version control history for this file may have further details.
 *
 * This source code is licensed under the GNU Lesser General Public License
 * version 2.1 or later (LGPLv2.1+) WITHOUT ANY WARRANTY.
 */

#ifndef PCMK__CRM_COMMON_DIGESTS_INTERNAL__H
#  define PCMK__CRM_COMMON_DIGESTS_INTERNAL__H

#include <libxml/tree.h>            // xmlNode

#ifdef __cplusplus
extern "C" {
#endif

// Digest comparison results
enum pcmk__digest_result {
    pcmk__digest_unknown,   // No digest available for comparison
    pcmk__digest_match,     // Digests match
    pcmk__digest_mismatch,  // Any parameter changed (potentially reloadable)
    pcmk__digest_restart,   // Parameters that require a restart changed
};

bool pcmk__verify_digest(xmlNode *input, const char *expected);

#ifdef __cplusplus
}
#endif

#endif // PCMK__CRM_COMMON_DIGESTS_INTERNAL__H
