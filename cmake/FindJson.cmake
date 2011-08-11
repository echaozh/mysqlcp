### FindJson.cmake -- try to find the json-c package

### Copyright (c) 2011 Zhang Yichao. All rights reserved.
### Author: Zhang Yichao <echaozh@gmail.com>
### Created: 2011-08-10
###

find_package (PkgConfig)
pkg_check_modules (PC_Json json)
set (Json_DEFINITIONS ${PC_Json_CFLAGS_OTHER})

find_path (Json_INCLUDE_DIRS json.h
  HINTS ${PC_Json_INCLUDEDIR} ${PC_Json_INCLUDE_DIRS}
  PATH_SUFFIXES json)

find_library (Json_LIBRARIES json
  HINTS ${PC_Json_LIBDIR} ${PC_Json_LIBRARY_DIRS})

include (FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set Json to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args (Json DEFAULT_MSG
  Json_LIBRARIES Json_INCLUDE_DIRS)

mark_as_advanced (Json_INCLUDE_DIRS Json_LIBRARIES)
