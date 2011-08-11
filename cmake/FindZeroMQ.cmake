### FindZeroMQ.cmake -- try to find the zeromq package

### Copyright (c) 2011 Zhang Yichao. All rights reserved.
### Author: Zhang Yichao <echaozh@gmail.com>
### Created: 2011-08-10
###

find_package (PkgConfig)
pkg_check_modules (PC_ZeroMQ libzmq)
set (ZeroMQ_DEFINITIONS ${PC_ZeroMQ_CFLAGS_OTHER})

find_path (ZeroMQ_INCLUDE_DIRS zmq.h
  HINTS ${PC_ZeroMQ_INCLUDEDIR} ${PC_ZeroMQ_INCLUDE_DIRS})

find_library (ZeroMQ_LIBRARIES zmq
  HINTS ${PC_ZeroMQ_LIBDIR} ${PC_ZeroMQ_LIBRARY_DIRS})

include (FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set ZeroMQ to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args (ZeroMQ DEFAULT_MSG
  ZeroMQ_LIBRARIES ZeroMQ_INCLUDE_DIRS)

mark_as_advanced (ZeroMQ_INCLUDE_DIRS ZeroMQ_LIBRARIES)
