cmake_minimum_required(VERSION 2.8)
include(LibFindMacros.cmake)

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

# libwebsockets

libfind_pkg_check_modules(libwebsockets_PKGCONF libwebsockets)

find_path(libwebsockets_INCLUDE_DIR
  NAMES libwebsockets.h
  PATHS ${libwebsockets_PKGCONF_INCLUDE_DIRS}
)

find_library(libwebsockets_LIBRARY
  NAMES websockets
  PATHS ${libwebsockets_PKGCONF_LIBRARY_DIRS}
)

set(libwebsockets_PROCESS_INCLUDES libwebsockets_INCLUDE_DIR)
set(libwebsockets_PROCESS_LIBS libwebsockets_LIBRARY)
libfind_process(libwebsockets)

include_directories(${libwebsockets_INCLUDE_DIR})
set(LIBS ${LIBS} ${libwebsockets_LIBRARY})

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

MESSAGE(STATUS "Building header files.")

execute_process(COMMAND "xxd" "-i" "raw_sql" "raw_sql.h"
                WORKING_DIRECTORY "../build")

add_executable(simple_server ../websocket/simple-server.c)

include_directories(${jansson_INCLUDE_DIR})
include_directories(../build/)
set(LIBS ${LIBS} ${jansson_LIBRARY})

target_link_libraries(simple_server ${LIBS})
