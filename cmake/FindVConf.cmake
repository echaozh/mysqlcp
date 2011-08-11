### FindVConf.cmake -- try to find the vconf package

### Copyright (c) 2011 Zhang Yichao. All rights reserved.
### Author: Zhang Yichao <echaozh@gmail.com>
### Created: 2011-08-10
###

find_package (PkgConfig)
pkg_check_modules (PC_VConf vconf)
set (VConf_DEFINITIONS ${PC_VConf_CFLAGS_OTHER})

find_path (VConf_INCLUDE_DIRS vconf.h
  HINTS ${PC_VConf_INCLUDEDIR} ${PC_VConf_INCLUDE_DIRS}
  PATH_SUFFIXES vconf)

find_library (VConf_LIBRARIES vconf
  HINTS ${PC_VConf_LIBDIR} ${PC_VConf_LIBRARY_DIRS})

include (FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set VConf to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args (VConf DEFAULT_MSG
  VConf_LIBRARIES VConf_INCLUDE_DIRS)

mark_as_advanced (VConf_INCLUDE_DIRS VConf_LIBRARIES)
