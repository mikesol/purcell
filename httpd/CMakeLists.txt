cmake_minimum_required(VERSION 2.8)
include(../cmake/LibFindMacros.cmake)

project(simple_server)

# SQLITE

libfind_pkg_check_modules(sqlite3_PKGCONF sqlite3)

find_path(sqlite3_INCLUDE_DIR
  NAMES sqlite3.h
  PATHS ${sqlite3_PKGCONF_INCLUDE_DIRS}
)

find_library(sqlite3_LIBRARY
  NAMES sqlite3
  PATHS ${sqlite3_PKGCONF_LIBRARY_DIRS}
)

set(sqlite3_PROCESS_INCLUDES sqlite3_INCLUDE_DIR)
set(sqlite3_PROCESS_LIBS sqlite3_LIBRARY)
libfind_process(sqlite3)
include_directories(${sqlite3_INCLUDE_DIR})
set(LIBS ${LIBS} ${sqlite3_LIBRARY})

# libmicrohttpd

libfind_pkg_check_modules(libmicrohttpd_PKGCONF libmicrohttpd)

find_path(libmicrohttpd_INCLUDE_DIR
  NAMES microhttpd.h
  PATHS ${libmicrohttpd_PKGCONF_INCLUDE_DIRS}
)

find_library(libmicrohttpd_LIBRARY
  NAMES microhttpd
  PATHS ${libmicrohttpd_PKGCONF_LIBRARY_DIRS}
)

set(libmicrohttpd_PROCESS_INCLUDES libmicrohttpd_INCLUDE_DIR)
set(libmicrohttpd_PROCESS_LIBS libmicrohttpd_LIBRARY)
libfind_process(libmicrohttpd)
include_directories(${libmicrohttpd_INCLUDE_DIR})
set(LIBS ${LIBS} ${libmicrohttpd_LIBRARY})

# jansson

libfind_pkg_check_modules(jansson_PKGCONF jansson)

find_path(jansson_INCLUDE_DIR
  NAMES jansson.h
  PATHS ${jansson_PKGCONF_INCLUDE_DIRS}
)

find_library(jansson_LIBRARY
  NAMES jansson
  PATHS ${jansson_PKGCONF_LIBRARY_DIRS}
)

set(jansson_PROCESS_INCLUDES jansson_INCLUDE_DIR)
set(jansson_PROCESS_LIBS jansson_LIBRARY)
libfind_process(jansson)
include_directories(${jansson_INCLUDE_DIR})
set(LIBS ${LIBS} ${jansson_LIBRARY})

MESSAGE(STATUS "Building header files.")

execute_process(COMMAND "xxd" "-i" "raw_sql" "raw_sql.h"
                WORKING_DIRECTORY "../build")

add_executable(simple_server ../httpd/simple-server.cc)

include_directories(../build/)

target_link_libraries(simple_server ${LIBS})
