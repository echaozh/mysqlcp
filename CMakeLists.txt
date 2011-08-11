cmake_minimum_required (VERSION 2.6)
project (mysqlcp)

set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

add_library (mysqlcp SHARED
  conn_pool.cpp mysql_conn.cpp sql_stmt.cpp mysql_stmt.cpp exception.cpp)
add_executable (mysqlcp-bin main.cpp)

add_definitions (-std=c++0x -Wall -Werror -D_GNU_SOURCE)

find_package (ZeroMQ REQUIRED)
find_package (Json REQUIRED)
find_package (VConf REQUIRED)
find_package (Boost COMPONENTS thread system REQUIRED)

include_directories (${ZeroMQ_INCLUDE_DIRS})
target_link_libraries (mysqlcp ${ZeroMQ_LIBRARIES})

include_directories (${Json_INCLUDE_DIRS})
target_link_libraries (mysqlcp ${Json_LIBRARIES})

include_directories (${VConf_INCLUDE_DIRS})
target_link_libraries (mysqlcp ${VConf_LIBRARIES})

find_path (MySQL_INCLUDE_DIRS mysql.h PATH_SUFFIXES mysql)
find_library (MySQL_LIBRARIES mysqlclient_r)
include_directories (mysqlcp ${MySQL_INCLUDE_DIRS})
target_link_libraries (mysqlcp ${MySQL_LIBRARIES})

include_directories (${Boost_INCLUDE_DIRS})
target_link_libraries (mysqlcp ${Boost_LIBRARIES})

target_link_libraries (mysqlcp-bin mysqlcp)

install (TARGETS mysqlcp LIBRARY DESTINATION lib)
install (TARGETS mysqlcp-bin RUNTIME DESTINATION bin)
install (FILES mysqlcp.conf sqls test_include DESTINATION etc)
