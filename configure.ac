dnl Process this file with autoconf to produce a configure script

AC_INIT([miw], [0.2])
AC_PREREQ(2.60)
AC_CONFIG_HEADERS([config.h])
AC_SUBST(ac_configure_args)
AM_INIT_AUTOMAKE([subdir-objects])

AC_PROG_CXX
#AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LIBTOOL

dnl Memory allocator
AC_CHECK_LIB([flow], [malloc], [have_flow=true], [have_flow=])
AC_CHECK_LIB([jemalloc], [malloc], [have_jemalloc=true], [have_jemalloc=])

AC_ARG_WITH([malloc],
    [AS_HELP_STRING([--with-malloc=TYPE], [Memory allocator (malloc|flow|jemalloc)])],
    [ac_cv_malloc=$withval], [ac_cv_malloc=yes])

if test \( "$ac_cv_malloc" = flow -a -z "$have_flow" \) -o \
        \( "$ac_cv_malloc" = jemalloc -a -z "$have_jemalloc" \) ; then
    AC_MSG_ERROR([$ac_cv_malloc not found])
fi
if test "$ac_cv_malloc" = yes ; then
    MEM_ALLOCATOR=
else
    MEM_ALLOCATOR=-l$ac_cv_malloc
fi

AC_SUBST([MEM_ALLOCATOR])

dnl map data structure. Configurable if not forced to use append according to metis mode
AC_ARG_ENABLE([map-ds],
              [AS_HELP_STRING([--enable-map-ds=ARG],
                              [default data structure for map phase: btree, array, append.
                               default: btree])],
              [ac_cv_map_ds=$enableval], [ac_cv_map_ds=btree])


dnl Configure metis mode. With single_XXX, Metis uses one bucket per mapper during map phase,
dnl sorts the output of the map phase using PSRS, then reduce the output. Grouping may
dnl happen before or after the PSRS.
ac_cv_all_modes="metis single_btree single_append-group_first single_append-merge_first"
AC_ARG_ENABLE([mode],
              [AS_HELP_STRING([--enable-mode=ARG],
                              [mode: $ac_cv_all_modes, default: metis])],
              [ac_cv_mode=$enableval], [ac_cv_mode=metis])

ac_cv_map_merge_reduce=false
if test "$ac_cv_mode" = single_btree ; then
    dnl grouped-map -> Merge -> Reduce
    ac_cv_map_merge_reduce=true
    ac_cv_map_ds=btree
elif test "$ac_cv_mode" = single_append-group_first ; then
    AC_DEFINE_UNQUOTED([SINGLE_APPEND_GROUP_FIRST], [1],
                       [Define to (Append-map -> Group) -> (psrs-merge, and-reduce)])
    ac_cv_map_merge_reduce=true
    ac_cv_map_ds=append
elif test "$ac_cv_mode" = single_append-merge_first ; then
    dnl Define to (Append-map) -> (psrs-merge, and Group-and-Reduce)]
    ac_cv_map_merge_reduce=true
    ac_cv_map_ds=append
fi

ac_cv_force_psrs=$ac_cv_map_merge_reduce
if test "$ac_cv_map_merge_reduce" = true; then
    AC_DEFINE_UNQUOTED([MAP_MERGE_REDUCE], [1], [map -> merge -> reduce])
fi

ac_cv_map_ds=index_$ac_cv_map_ds
AC_DEFINE_UNQUOTED([DEFAULT_MAP_DS], [$ac_cv_map_ds], [Define data structure for map phase])

dnl Sort algorithm. Configurable only if not forced to use psrs
ac_cv_sort=psrs
AC_ARG_ENABLE([sort],
              [AS_HELP_STRING([--enable-sort=ARG],
                               [mode: psrs or mergesort, default: psrs])],
              [ac_cv_sort=$enableval], [ac_cv_sort=psrs])
if test "$ac_cv_sort" = psrs ; then
    AC_DEFINE_UNQUOTED([USE_PSRS], [1], [Define if you want to use psrs for sorting])
elif test "$ac_cv_force_psrs" = true ; then
    AC_MSG_ERROR([Metis must use psrs to sort in current mode])
else
    AC_DEFINE_UNQUOTED([USE_PSRS], [0], [Define if you want to use psrs for sorting])
fi

# Check if there is google-gflags library installed.
AC_ARG_WITH(gflags, AS_HELP_STRING[--with-gflags=GFLAGS_DIR],
  GFLAGS_CFLAGS="-I${with_gflags}/include"
  GFLAGS_LIBS="-L${with_gflags}/lib -lgflags"
  CFLAGS="$CFLAGS $GFLAGS_CFLAGS"
  LIBS="$LIBS $GFLAGS_LIBS"
)
AC_CHECK_LIB(gflags, main, ac_cv_have_libgflags=1, ac_cv_have_libgflags=0)
if test x"$ac_cv_have_libgflags" = x"1"; then
   AC_DEFINE(HAVE_LIB_GFLAGS, 1, [define if you have google gflags library])
   if test x"$GFLAGS_LIBS" = x""; then
      GFLAGS_LIBS="-lgflags"
   fi
else
  GFLAGS_CFLAGS=
  GFLAGS_LIBS=
fi
AC_SUBST(GFLAGS_LIBS)
AC_SUBST(GFLAGS_CFLAGS)
AM_CONDITIONAL(HAVE_GFLAGS, test x"$ac_cv_have_libgflags" = x"1")

# Check if there is google-glog library installed.
AC_ARG_ENABLE(gglog,
AC_HELP_STRING([--enable-gglog],[Enable google glog (default is NO)]),
[case "${enableval}" in 
	yes) gglog=true ;;
	no) gglog=false ;;
	*) AC_MSG_ERROR(bad_value ${enableval} for --enable-gglog) ;;
esac],[gglog=false])
if test x"$gglog" = x"true"; then
   AC_DEFINE(HAVE_GLOG,1,[google glog requested])
fi

AC_ARG_WITH(glog, AS_HELP_STRING[--with-glog=GLOG_DIR],
  GLOG_CFLAGS="-I${with_glog}/include"
  GLOG_LIBS="-L${with_glog}/lib -lglog"
  CFLAGS="$CFLAGS $GLOG_CFLAGS"
  LIBS="$LIBS $GLOG_LIBS"
)
AC_CHECK_LIB(glog, main, ac_cv_have_libglog=1, ac_cv_have_libglog=0)
if test x"$ac_cv_have_libglog" = x"1"; then
   AC_DEFINE(HAVE_LIB_GLOG, 1, [define if you have google glog library])
   if test x"$GLOG_LIBS" = x""; then
      GLOG_LIBS="-lglog"
   fi
else
  GLOG_CFLAGS=
  GLOG_LIBS=
fi
AC_SUBST(GLOG_LIBS)

dnl Debug mode
AC_ARG_ENABLE([debug],
              [AS_HELP_STRING([--enable-debug],
                              [mode: -O0 in debug mode; -O3 otherwise, default: false])],
              [ac_cv_debug=true], [ac_cv_debug=false])

if test "$ac_cv_debug" = true ; then
    OPT_LEVEL=-O0
else
    OPT_LEVEL=-O3
fi

dnl Profiling
AC_ARG_ENABLE([profile],
              [AS_HELP_STRING([--enable-profile],
                              [profile: enable performmance counters (see lib/mr-prof.c), default: false])],
              [ac_cv_profile=true], [ac_cv_profile=false])

if test "$ac_cv_profile" = true ; then
    AC_DEFINE_UNQUOTED([PROFILE_ENABLED], [1],
                       [Define if you want metis to print out profiling results from performance counters])
fi


dnl constexpr
AC_CACHE_CHECK([whether the C++ compiler understands constexpr], [ac_cv_cxx_constexpr], [
               AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[constexpr int f(int x) { return x + 1; }]], [[]])],
               [ac_cv_cxx_constexpr=yes], [ac_cv_cxx_constexpr=no])])
if test "$ac_cv_cxx_constexpr" = yes; then
    AC_DEFINE([HAVE_CXX_CONSTEXPR], [1], [Define if the C++ compiler understands constexpr.])
fi

dnl protobuf
PKG_CHECK_MODULES([protobuf],[protobuf >= 2.2],protobuf=true,protobuf=false)
if test $protobuf = true ; then
  AC_DEFINE(PROTOBUF,1,"Protocol buffers available")
fi
AM_CONDITIONAL(HAVE_PROTOBUF,test x$protobuf = xtrue)
if test $protobuf = false ; then
   AC_MSG_ERROR([Protocol buffers are required])
fi

dnl jsoncpp
PKG_CHECK_MODULES([jsoncpp],[jsoncpp],jsoncpp=true,jsoncpp=false)
if test $jsoncpp = true ; then
  AC_DEFINE(JSONCPP,1,"Protocol buffers available")
fi
AM_CONDITIONAL(HAVE_JSONCPP,test x$jsoncpp = xtrue)
if test $jsoncpp = false ; then
   AC_MSG_ERROR([JSON cpp (http://jsoncpp.sourceforge.net/) lib is required])
fi

dnl snappy
AC_CHECK_HEADERS([snappy.h])

AC_SUBST([OPT_LEVEL])

AC_CONFIG_FILES(Makefile miw/Makefile miw/formats/Makefile metis/Makefile app/Makefile tests/Makefile)

AC_OUTPUT
