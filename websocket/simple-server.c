#include <stdio.h>
#include <stdlib.h>
#include <libwebsockets.h>
#include <string.h>
#include <sqlite3.h>
#include <jansson.h>

#include "raw_sql.h"

static int sql_length = 0;
static char * sql_success = NULL;
static char *PASSWORD;

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

static int loadSave_sql_database(const char *zFilename, const char *password, sqlite3 *pInMemory, int isSave) {//printf("zFilename %s\n", zFilename);
    if (isSave && password && (strcmp(password, PASSWORD) != 0)) {
      printf("failed validation");
      return -1;
    }
    
    int rc;                   /* Function return code */
    sqlite3 *pFile;           /* Database connection opened on zFilename */
    sqlite3_backup *pBackup;  /* Backup object used to copy data */
    sqlite3 *pTo;             /* Database to copy to (pFile or pInMemory) */
    sqlite3 *pFrom;           /* Database to copy from (pFile or pInMemory) */

    /* Open the database file identified by zFilename. Exit early if this fails
    ** for any reason. */
    rc = sqlite3_open(zFilename, &pFile);
    if( rc==SQLITE_OK ){

      /* If this is a 'load' operation (isSave==0), then data is copied
      ** from the database file just opened to database pInMemory. 
      ** Otherwise, if this is a 'save' operation (isSave==1), then data
      ** is copied from pInMemory to pFile.  Set the variables pFrom and
      ** pTo accordingly. */
      pFrom = (isSave ? pInMemory : pFile);
      pTo   = (isSave ? pFile     : pInMemory);

      /* Set up the backup procedure to copy from the "main" database of 
      ** connection pFile to the main database of connection pInMemory.
      ** If something goes wrong, pBackup will be set to NULL and an error
      ** code and  message left in connection pTo.
      **
      ** If the backup object is successfully created, call backup_step()
      ** to copy data from pFile to pInMemory. Then call backup_finish()
      ** to release resources associated with the pBackup object.  If an
      ** error occurred, then  an error code and message will be left in
      ** connection pTo. If no error occurred, then the error code belonging
      ** to pTo is set to SQLITE_OK.
      */
      pBackup = sqlite3_backup_init(pTo, "main", pFrom, "main");
      if( pBackup ){
        (void)sqlite3_backup_step(pBackup, -1);
        (void)sqlite3_backup_finish(pBackup);
      }
      rc = sqlite3_errcode(pTo);
    }
    return rc;
}


static void initialize_sql_database_to_blank(sqlite3* sql_database) {
    int rc;
    char *zErrMsg = 0;
    char *full_sql_program = NULL;
    full_sql_program = malloc(raw_sql_len + 1);
    memcpy(full_sql_program, raw_sql, raw_sql_len);
    full_sql_program[raw_sql_len] = '\0';
    rc = sqlite3_exec(sql_database, full_sql_program, callback_vanilla, 0, &zErrMsg);
    if( rc!=SQLITE_OK )
    {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    free(full_sql_program);
}

static int callback_purcell(struct libwebsocket_context * context,
                                      struct libwebsocket *wsi,
                                      enum libwebsocket_callback_reasons reason,
                                      void *user, void *in, size_t len)
{
    sqlite3 ** sql_database = (sqlite3 **) (user);
    switch (reason)
    {
    case LWS_CALLBACK_ESTABLISHED:
    {
        printf("connection established\n");
        
        int rc;

        rc = sqlite3_open(":memory:", sql_database);
        if( rc )
        {
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(*sql_database));
            sqlite3_close(*sql_database);
            return(1);
        }
        break;
    }
    case LWS_CALLBACK_SERVER_WRITEABLE:
    {
        // no checking user id because it doesn't matter
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
        char *in_json = NULL;
        char* initializing = NULL;
        char *global_cache_path = NULL;
        char *global_password = NULL;

        in_json = malloc(len + 1);
        memcpy(in_json, in, len);
        in_json[len] = '\0';
        json_error_t json_error;
        json_t *request_root = json_loads( in_json, 0, &json_error );
        json_t *response_root = json_object();
        int just_me = 1;

        if( request_root )
        {
            json_t *jsonReturn = json_object_get( request_root, "return" );
            if (json_is_string( jsonReturn ))
            {
                const char *perhaps_unnecessary = json_string_value( jsonReturn );
                just_me = strcmp(perhaps_unnecessary, "everyone") != 0;
            }
            json_t *jsonInitializing = json_object_get( request_root, "initialize" );
            if (json_is_string( jsonInitializing ))
            {
                const char *sql_from_json = json_string_value( jsonInitializing );
                initializing = malloc(strlen(sql_from_json) + 1);
                strcpy(initializing, sql_from_json);
            }
            //////////////////
            json_t *jsonPassword = json_object_get( request_root, "password" );
            if( json_is_string( jsonPassword ) )
            {
                const char *sql_from_json = json_string_value( jsonPassword );
                global_password = malloc(strlen(sql_from_json) + 1);
                strcpy(global_password, sql_from_json);
            }
            //////////////////
            json_t *jsonCachePath = json_object_get( request_root, "cache_path" );
            if( json_is_string( jsonCachePath ) )
            {
                const char *sql_from_json = json_string_value( jsonCachePath );
                global_cache_path = malloc(strlen(sql_from_json) + 1);
                strcpy(global_cache_path, sql_from_json);
            }
            if (initializing)
            {
              if (strcmp(initializing, "") == 0) {
                initialize_sql_database_to_blank(*sql_database);
              } else {
                rc = loadSave_sql_database(initializing, 0, *sql_database, 0);
                if (rc != SQLITE_OK) {
                  printf("could not initialize from database - creating blank");
                  initialize_sql_database_to_blank(*sql_database);
                }
              }
            }
            else
            {
                json_t *jsonArr = json_object_get( request_root, "sql" );
                if (json_is_array( jsonArr ))
                {
                    const unsigned int length = json_array_size( jsonArr );
                    unsigned int idx = 0;
                    for (idx = 0; idx < length; idx++)
                    {
                        char *sql_request = NULL;
                        char *request_name = NULL;
                        char *cache_path = NULL;
                        char *password = NULL;
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
                        jsonData = json_object_get( jsonObject, "password" );
                        if( json_is_string( jsonData ) )
                        {
                            const char *sql_from_json = json_string_value( jsonData );
                            password = malloc(strlen(sql_from_json) + 1);
                            strcpy(password, sql_from_json);
                        }
                        //////////////////
                        jsonData = json_object_get( jsonObject, "cache_path" );
                        if( json_is_string( jsonData ) )
                        {
                            const char *sql_from_json = json_string_value( jsonData );
                            cache_path = malloc(strlen(sql_from_json) + 1);
                            strcpy(cache_path, sql_from_json);
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
                        rc = sqlite3_exec(*sql_database, sql_request, callback_sql, sql_json_arr, &zErrMsg);
                        if( rc!=SQLITE_OK )
                        {
                            fprintf(stderr, "SQL error: %s\n", zErrMsg);
                            sqlite3_free(zErrMsg);
                        }
                        else
                        {
                            //printf("SQL success!\n");
                        }

                        if (cache_path) {
                          rc = loadSave_sql_database(cache_path, password, *sql_database, 1);
                          if( rc!=SQLITE_OK )
                          {
                              printf("Caching did not work.\n");
                          }
                          
                        }
                        json_object_set_new( response_root, request_name ? request_name : "anonymous", sql_json_arr );
                        if (sql_request) {
                          free(sql_request);
                        }
                        if (request_name) {
                          free(request_name);
                        }
                        if (cache_path) {
                          free(cache_path);
                        }
                        if (password) {
                          free(password);
                        }
                    }
                }
            }
            if (global_cache_path) {
              rc = loadSave_sql_database(global_cache_path, global_password, *sql_database, 1);
              if( rc!=SQLITE_OK )
              {
                  printf("Caching did not work.\n");
              }
              
            }
            json_t *jsonSubsequent = json_object_get( request_root, "subsequent" );
            if (json_is_string( jsonSubsequent ))
            {
                // hmmm...overkill?
                const char *perhaps_unnecessary = json_string_value( jsonSubsequent );
                json_object_set_new( response_root, "subsequent", json_string( perhaps_unnecessary ) );
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

        if (in_json) {
          free(in_json);
        }
        if (initializing) {
          free(initializing);
        }
        //if (just_me)
        //{
           libwebsocket_callback_on_writable(context, wsi);
        //}
        //else
        //{
        //   libwebsocket_callback_on_writable_all_protocol(
        //              libwebsockets_get_protocol(wsi));
        //}
        break;
    }
    case LWS_CALLBACK_CLOSED:
    {
      printf("connection closed\n");
      sqlite3_close(*sql_database);
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
        0,              // nothing, since we never use http
    },
    {
        "purcell-engraving-protocol", // protocol name - very important!
        callback_purcell,   // callback
        sizeof(sqlite3 *) // user is identified by a pointer to its database
    },
    {
        NULL, NULL, 0
    }
};

long slurp(char const* path, char **buf, int add_nul)
{
    FILE  *fp;
    size_t fsz;
    long   off_end;
    int    rc;

    /* Open the file */
    fp = fopen(path, "r");
    if( NULL == fp ) {
        return -1L;
    }

    /* Seek to the end of the file */
    rc = fseek(fp, 0L, SEEK_END);
    if( 0 != rc ) {
        return -1L;
    }

    /* Byte offset to the end of the file (size) */
    if( 0 > (off_end = ftell(fp)) ) {
        return -1L;
    }
    fsz = (size_t)off_end;

    /* Allocate a buffer to hold the whole file */
    *buf = malloc( fsz+add_nul );
    if( NULL == *buf ) {
        return -1L;
    }

    /* Rewind file pointer to start of file */
    rewind(fp);

    /* Slurp file into buffer */
    if( fsz != fread(*buf, 1, fsz, fp) ) {
        free(*buf);
        return -1L;
    }

    /* Close the file */
    if( EOF == fclose(fp) ) {
        free(*buf);
        return -1L;
    }

    if( add_nul ) {
        /* Make sure the buffer is NUL-terminated, just in case */
        (*buf)[fsz] = '\0';
    }

    /* Return the file size */
    return (long)fsz;
}


int main(void)
{
    // server url will be http://localhost:9000
    int port = 9000;
    slurp("password.txt",&PASSWORD,1);
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
    if (PASSWORD) {
      free(PASSWORD);
    }
    return 0;
}