#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for src/common
#
# This makefile generates two outputs:
#
#	libpgcommon.a - contains object files with FRONTEND defined,
#		for use by client application and libraries
#
#	libpgcommon_srv.a - contains object files without FRONTEND defined,
#		for use only by the backend binaries
#
# IDENTIFICATION
#    src/common/Makefile
#
#-------------------------------------------------------------------------

subdir = src/common
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)
LIBS += $(PTHREAD_LIBS)

OBJS_COMMON = exec.o pg_lzcompress.o pgfnames.o psprintf.o relpath.o \
	rmtree.o string.o username.o wait_error.o

OBJS_FRONTEND = $(OBJS_COMMON) fe_memutils.o restricted_token.o

OBJS_SRV = $(OBJS_COMMON:%.o=%_srv.o)

all: libpgcommon.a libpgcommon_srv.a

# libpgcommon is needed by some contrib
install: all installdirs
	$(INSTALL_STLIB) libpgcommon.a '$(DESTDIR)$(libdir)/libpgcommon.a'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)'

uninstall:
	rm -f '$(DESTDIR)$(libdir)/libpgcommon.a'

libpgcommon.a: $(OBJS_FRONTEND)
	rm -f $@
	$(AR) $(AROPT) $@ $^

#
# Server versions of object files
#

libpgcommon_srv.a: $(OBJS_SRV)
	rm -f $@
	$(AR) $(AROPT) $@ $^

# Because this uses its own compilation rule, it doesn't use the
# dependency tracking logic from Makefile.global.  To make sure that
# dependency tracking works anyway for the *_srv.o files, depend on
# their *.o siblings as well, which do have proper dependencies.  It's
# a hack that might fail someday if there is a *_srv.o without a
# corresponding *.o, but it works for now.
ifeq ($(genmsgids), yes)
PGXL_MSG_FILEID := 1
%_srv.o: %.c %.o
	$(CC) $(CFLAGS) -DPGXL_MSG_MODULE=$(PGXL_MSG_MODULE) -DPGXL_MSG_FILEID=$(PGXL_MSG_FILEID) $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@
	$(eval PGXL_MSG_FILEID := $(shell expr $(PGXL_MSG_FILEID) + 1))
else
%_srv.o: %.c %.o
	$(CC) $(CFLAGS) $(subst -DFRONTEND,, $(CPPFLAGS)) -c $< -o $@
endif

$(OBJS_SRV): | submake-errcodes

.PHONY: submake-errcodes

submake-errcodes:
	$(MAKE) -C ../backend submake-errcodes

clean distclean maintainer-clean:
	rm -f libpgcommon.a libpgcommon_srv.a $(OBJS_FRONTEND) $(OBJS_SRV)
