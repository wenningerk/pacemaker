#
# Copyright 2020-2024 the Pacemaker project contributors
#
# The version control history for this file may have further details.
#
# This source code is licensed under the GNU General Public License version 2
# or later (GPLv2+) WITHOUT ANY WARRANTY.
#

include $(top_srcdir)/mk/common.mk

# tap-test is copied from /usr/share/automake-*/tap-driver.sh.
EXTRA_DIST = tap-driver.sh 	\
	     tap-test 		\
	     test-headers.sh

.PHONY: check
check: check-headers

.PHONY: check-headers
check-headers:
	CFLAGS="$(CFLAGS)" CXXFLAGS="$(CXXFLAGS)" CC="$(CC)"		\
		CXX="$(CXX)" CPPFLAGS="$(CPPFLAGS) -I$(top_builddir)/include" LIBS="$(LIBS)"	\
		SRCDIR="$(top_srcdir)" sh "$(srcdir)/test-headers.sh"
