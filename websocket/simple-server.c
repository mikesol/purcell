#include <stdio.h>
#include <stdlib.h>
#include <libwebsockets.h>
#include <string.h>
#include <sqlite3.h>
#include <jansson.h>

#include "raw_sql.h"

static sqlite3 *db;

static int sql_length = 0;
static char * sql_success = NULL;
static int score_initialized = 0;

static int callback_sql(void *result_json_arr_v, int argc, char **argv, char **azColName)
{

    json_t * result_json_arr = (json_t *) result_json_arr_v;
    json_t *leaf = json_object();
    int i;
    for(i=0; i<argc; i++)
    {
        json_object_set_new( leaf, azColName[i], json_string( argv[i] ? argv[i] : "NULL" ) );
    }
    json_array_append_new(result_json_arr, leaf);
    return 0;
}

static int callback_vanilla(void *NotUsed, int argc, char **argv, char **azColName)
{
    return 0;
}

static int callback_http(struct libwebsocket_context * context,
                         struct libwebsocket *wsi,
                         enum libwebsocket_callback_reasons reason, void *user,
                         void *in, size_t len)
{
    return 0;
}

static int callback_purcell(struct libwebsocket_context * context,
                                      struct libwebsocket *wsi,
                                      enum libwebsocket_callback_reasons reason,
                                      void *user, void *in, size_t len)
{
    switch (reason)
    {
    case LWS_CALLBACK_ESTABLISHED:
    {
        printf("connection established\n");
        break;
    }
    case LWS_CALLBACK_SERVER_WRITEABLE:
    {
        unsigned char *buf = (unsigned char*) malloc(LWS_SEND_BUFFER_PRE_PADDING + sql_length + 1 +
                             LWS_SEND_BUFFER_POST_PADDING);

        int i=0;

        for (i=0; i < sql_length + 1; i++)
        {
            buf[LWS_SEND_BUFFER_PRE_PADDING + i] = sql_success[i];
        }

        libwebsocket_write(wsi, &buf[LWS_SEND_BUFFER_PRE_PADDING], sql_length, LWS_WRITE_TEXT);

        // release memory back into the wild
        free(buf);
        break;
    }
    case LWS_CALLBACK_RECEIVE:
    {
        int rc;
        char *in_json;
        in_json = malloc(len + 1);
        memcpy(in_json, in, len);
        in_json[len] = '\0';
        json_error_t json_error;
        json_t *request_root = json_loads( in_json, 0, &json_error );
        json_t *response_root = json_object();
        int initializing = 0;
        int just_me = 1;

        if( request_root )
        {
            json_t *jsonReturn = json_object_get( request_root, "return" );
            if (json_is_string( jsonReturn ))
            {
                const char *perhaps_unnecessary = json_string_value( jsonReturn );
                just_me = strcmp(perhaps_unnecessary, "everyone") != 0;
            }
            json_t *jsonInitializing = json_object_get( request_root, "initializing" );
            if (json_is_boolean( jsonInitializing ))
            {
                initializing = json_integer_value ( jsonInitializing );
            }
            if (( initializing == 0 ) | ((initializing == 1) && (score_initialized == 0)))
            {
                score_initialized = 1;
                json_t *jsonArr = json_object_get( request_root, "sql" );
                if (json_is_array( jsonArr ))
                {
                    const unsigned int length = json_array_size( jsonArr );
                    unsigned int idx = 0;
                    for (idx = 0; idx < length; idx++)
                    {
                        char *sql_request = NULL;
                        char *request_name = NULL;
                        json_t *jsonObject = json_array_get( jsonArr, idx );
                        json_t *sql_json_arr = json_array();
                        json_t *jsonData = json_object_get( jsonObject, "sql" );
                        char *zErrMsg = 0;
                        if( json_is_string( jsonData ) )
                        {
                            const char *sql_from_json = json_string_value( jsonData );
                            sql_request = malloc(strlen(sql_from_json) + 1);
                            strcpy(sql_request, sql_from_json);
                        }
                        //////////////////
                        jsonData = json_object_get( jsonObject, "name" );
                        if( json_is_string( jsonData ) )
                        {
                            const char *sql_from_json = json_string_value( jsonData );
                            request_name = malloc(strlen(sql_from_json) + 1);
                            strcpy(request_name, sql_from_json);
                        }
                        //printf("EXECUTING: %s\n", sql_request);
                        rc = sqlite3_exec(db, sql_request, callback_sql, sql_json_arr, &zErrMsg);
                        if( rc!=SQLITE_OK )
                        {
                            fprintf(stderr, "SQL error: %s\n", zErrMsg);
                            sqlite3_free(zErrMsg);
                        }
                        else
                        {
                            //printf("SQL success!\n");
                        }

                        json_object_set_new( response_root, request_name ? request_name : "anonymous", sql_json_arr );
                        free(sql_request);
                        free(request_name);
                    }
                }
                json_t *jsonSubsequent = json_object_get( request_root, "subsequent" );
                if (json_is_string( jsonSubsequent ))
                {
                    // hmmm...overkill?
                    const char *perhaps_unnecessary = json_string_value( jsonSubsequent );
                    json_object_set_new( response_root, "subsequent", json_string( perhaps_unnecessary ) );
                }
            }
            // free json!
            json_decref( request_root );
        }
        else
        {
            printf("JSON ERR %s\n", json_error.text);
            printf("JSON ERR SRC %s\n", json_error.source);
            printf("JSON ERR LINE %d\n", json_error.line);
            printf("JSON ERR COL %d\n", json_error.column);
            printf("JSON ERR POS %d\n", json_error.position);
        }

        if (sql_success) {
          free(sql_success);
        }
        sql_success = json_dumps( response_root, 0 );
        json_decref( response_root );
        sql_length = strlen(sql_success);

        free(in_json);
        if (just_me)
        {
           libwebsocket_callback_on_writable(context, wsi);
        }
        else
        {
           libwebsocket_callback_on_writable_all_protocol(
                      libwebsockets_get_protocol(wsi));
        }
        break;
    }
    default:
        break;
    }


    return 0;
}

static struct libwebsocket_protocols protocols[] =
{
    /* first protocol must always be HTTP handler */
    {
        "http-only",   // name
        callback_http, // callback
        0,              // per_session_data_size
        0
    },
    {
        "purcell-engraving-protocol", // protocol name - very important!
        callback_purcell,   // callback
        0,                          // we don't use any per session data
        0

    },
    {
        NULL, NULL, 0, 0   /* End of list */
    }
};

int main(void)
{
    // server url will be http://localhost:9000
    int port = 9000;
    const char *interface = NULL;
    struct libwebsocket_context *context;
    // we're not using ssl
    const char *cert_path = NULL;
    const char *key_path = NULL;
    // no special options
    int opts = 0;

    // create libwebsocket context representing this server
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof info);

    info.port = port;
    info.iface = interface;
    info.protocols = protocols;
    info.extensions = libwebsocket_get_internal_extensions();
    info.ssl_cert_filepath = cert_path;
    info.ssl_private_key_filepath = key_path;
    info.gid = -1;
    info.uid = -1;
    info.options = opts;
    info.user = NULL;
    info.ka_time = 0;
    info.ka_probes = 0;
    info.ka_interval = 0;


    char *zErrMsg = 0;
    int rc;

    rc = sqlite3_open(":memory:", &db);
    if( rc )
    {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return(1);
    }

    char *full_sql_program = NULL;
    full_sql_program = malloc(raw_sql_len + 1);
    memcpy(full_sql_program, raw_sql, raw_sql_len);
    full_sql_program[raw_sql_len] = '\0';
    rc = sqlite3_exec(db, full_sql_program, callback_vanilla, 0, &zErrMsg);
    if( rc!=SQLITE_OK )
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    free(full_sql_program);

    context = libwebsocket_create_context(&info);
    if (context == NULL)
    {
        fprintf(stderr, "libwebsocket init failed\n");
        return -1;
    }

    printf("starting server...\n");

    // infinite loop, to end this server send SIGTERM. (CTRL+C)
    while (1)
    {
        libwebsocket_service(context, 50);
        // libwebsocket_service will process all waiting events with their
        // callback functions and then wait 50 ms.
        // (this is a single threaded webserver and this will keep our server
        // from generating load while there are not requests to process)
    }

    libwebsocket_context_destroy(context);
    sqlite3_close(db);
    return 0;
}