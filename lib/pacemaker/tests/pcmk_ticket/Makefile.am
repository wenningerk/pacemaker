#
# Copyright 2024 the Pacemaker project contributors
#
# The version control history for this file may have further details.
#
# This source code is licensed under the GNU General Public License version 2
# or later (GPLv2+) WITHOUT ANY WARRANTY.
#

include $(top_srcdir)/mk/common.mk
include $(top_srcdir)/mk/tap.mk
include $(top_srcdir)/mk/unittest.mk

LDADD += $(top_builddir)/lib/pacemaker/libpacemaker.la
LDADD += $(top_builddir)/lib/cib/libcib.la

# Add "_test" to the end of all test program names to simplify .gitignore.

check_PROGRAMS = pcmk__get_ticket_state_test \
		 pcmk_ticket_constraints_test \
		 pcmk_ticket_delete_test \
		 pcmk_ticket_get_attr_test \
		 pcmk_ticket_info_test \
		 pcmk_ticket_remove_attr_test \
		 pcmk_ticket_set_attr_test \
		 pcmk_ticket_state_test

TESTS = $(check_PROGRAMS)
