#include <stdlib.h>
#include <string.h> 	
#include <stdio.h> 	
#include <sys/types.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <microhttpd.h>
#include <string>

#include <sqlite3.h>
#include <jansson.h>

#include <vector>
 
#include "raw_sql.h"

#define PORT            9000
#define POSTBUFFERSIZE  512

#define MAXSiZE 10000

#define GET             0
#define POST            1

using namespace std;

vector<pair<string, sqlite3** > > SQLITE_DATABASES;

static char *PASSWORD;

////// sqlite
static int loadSave_sql_database(const char *zFilename, const char *password, sqlite3 *pInMemory, int isSave) {
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


static int callback_vanilla(void *NotUsed, int argc, char **argv, char **azColName)
{
    return 0;
}

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

static void initialize_sql_database_to_blank(sqlite3* sql_database) {
    int rc;
    char *zErrMsg = 0;
    char *full_sql_program = NULL;
    full_sql_program = (char *) malloc(raw_sql_len + 1);
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


///////

struct connection_info_struct
{
  int connectiontype;
  struct MHD_PostProcessor *postprocessor;
  string answerjson;
  int answercode;
};

sqlite3 ** get_sqlite_database_for_client (string client)
{
  for (int i = 0; i < SQLITE_DATABASES.size(); i++) {
    if (SQLITE_DATABASES[i].first == client) {
      return SQLITE_DATABASES[i].second;
    }
  }
  
  SQLITE_DATABASES.push_back(pair<string, sqlite3 **> (client, (sqlite3 **)malloc(sizeof(sqlite3 *))));
  sqlite3_open(":memory:", SQLITE_DATABASES[SQLITE_DATABASES.size() - 1].second);

  if (SQLITE_DATABASES.size() >= MAXSiZE) {
    sqlite3_close(*SQLITE_DATABASES[0].second);
    SQLITE_DATABASES.erase(SQLITE_DATABASES.begin());
  }
  
  return SQLITE_DATABASES[SQLITE_DATABASES.size() - 1].second;
}

static int
send_json (struct MHD_Connection *connection, string json,
           int status_code)
{
  int ret;
  struct MHD_Response *response;

  const char *cstring_version_of_json = json.c_str();

  response =
    MHD_create_response_from_buffer (json.size(), (void *) cstring_version_of_json,
				     MHD_RESPMEM_MUST_COPY);
  if (!response)
    return MHD_NO;

  MHD_add_response_header (response, MHD_HTTP_HEADER_CONTENT_TYPE, "application/json");
  MHD_add_response_header (response, MHD_HTTP_HEADER_ACCESS_CONTROL_ALLOW_ORIGIN, "*");

  ret = MHD_queue_response (connection, status_code, response);
  MHD_destroy_response (response);

  return ret;
}



static int 
process_and_send_json (struct MHD_Connection *connection, string json,
           int status_code)
{
  int rc;
  char *in_json = NULL;
  char* initializing = NULL;
  char* client = NULL;
  char *global_cache_path = NULL;
  char *global_password = NULL;

  in_json = (char *) malloc(json.size() + 1);
  strcpy(in_json, json.c_str());
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
      ////////////
      json_t *jsonClient = json_object_get( request_root, "client" );
      if (json_is_string( jsonClient ))
      {
          const char *sql_from_json = json_string_value( jsonClient );
          client = (char *) malloc(strlen(sql_from_json) + 1);
          strcpy(client, sql_from_json);
      }
      ////////////
      json_t *jsonInitializing = json_object_get( request_root, "initialize" );
      if (json_is_string( jsonInitializing ))
      {
          const char *sql_from_json = json_string_value( jsonInitializing );
          initializing = (char *) malloc(strlen(sql_from_json) + 1);
          strcpy(initializing, sql_from_json);
      }
      //////////////////
      json_t *jsonPassword = json_object_get( request_root, "password" );
      if( json_is_string( jsonPassword ) )
      {
          const char *sql_from_json = json_string_value( jsonPassword );
          global_password = (char *) malloc(strlen(sql_from_json) + 1);
          strcpy(global_password, sql_from_json);
      }
      //////////////////
      json_t *jsonCachePath = json_object_get( request_root, "cache_path" );
      if( json_is_string( jsonCachePath ) )
      {
          const char *sql_from_json = json_string_value( jsonCachePath );
          global_cache_path = (char *) malloc(strlen(sql_from_json) + 1);
          strcpy(global_cache_path, sql_from_json);
      }
      sqlite3 ** sql_database = get_sqlite_database_for_client (client);
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
                      sql_request = (char *) malloc(strlen(sql_from_json) + 1);
                      strcpy(sql_request, sql_from_json);
                  }
                  //////////////////
                  jsonData = json_object_get( jsonObject, "password" );
                  if( json_is_string( jsonData ) )
                  {
                      const char *sql_from_json = json_string_value( jsonData );
                      password = (char *) malloc(strlen(sql_from_json) + 1);
                      strcpy(password, sql_from_json);
                  }
                  //////////////////
                  jsonData = json_object_get( jsonObject, "cache_path" );
                  if( json_is_string( jsonData ) )
                  {
                      const char *sql_from_json = json_string_value( jsonData );
                      cache_path = (char *) malloc(strlen(sql_from_json) + 1);
                      strcpy(cache_path, sql_from_json);
                  }
                  //////////////////
                  jsonData = json_object_get( jsonObject, "name" );
                  if( json_is_string( jsonData ) )
                  {
                      const char *sql_from_json = json_string_value( jsonData );
                      request_name = (char *) malloc(strlen(sql_from_json) + 1);
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

  char * sql_success = json_dumps( response_root, 0 );

  json_decref( response_root );

  if (in_json) {
    free(in_json);
  }
  if (initializing) {
    free(initializing);
  }
  if (client) {
    free(client);
  }
  
  if (sql_success) {
    string out_json(sql_success);
    free(sql_success);
    return send_json(connection, out_json, status_code);
  }
  return send_json(connection, "{\"error\" : \"something went wrong with sql parsing\" }", status_code);
}

static int
iterate_post (void *coninfo_cls, enum MHD_ValueKind kind, const char *key,
              const char *filename, const char *content_type,
              const char *transfer_encoding, const char *data, uint64_t off,
              size_t size)
{
  struct connection_info_struct *con_info = (connection_info_struct *) coninfo_cls;

  if (0 != strcmp (key, "data")) {
    con_info->answerjson = "{\"error\" : \"there was an error with the server\"}";
    con_info->answercode = MHD_HTTP_INTERNAL_SERVER_ERROR;
    return MHD_NO;
  }

  string data_in_string_form (data, size); // copy data at a given size

  con_info->answerjson += data_in_string_form;
  con_info->answercode = MHD_HTTP_OK;

  return MHD_YES;
}

static void
request_completed (void *cls, struct MHD_Connection *connection,
                   void **con_cls, enum MHD_RequestTerminationCode toe)
{
  struct connection_info_struct *con_info = (connection_info_struct *) *con_cls;

  if (NULL == con_info)
    return;

  if (con_info->connectiontype == POST)
    {
      if (NULL != con_info->postprocessor)
        {
          MHD_destroy_post_processor (con_info->postprocessor);
        }
    }

  free (con_info);
  *con_cls = NULL;
}


static int
answer_to_connection (void *cls, struct MHD_Connection *connection,
                      const char *url, const char *method,
                      const char *version, const char *upload_data,
                      size_t *upload_data_size, void **con_cls)
{
  if (NULL == *con_cls)
    {
      struct connection_info_struct *con_info;

      con_info = new connection_info_struct();
      if (NULL == con_info)
        return MHD_NO;

      if (0 == strcmp (method, "POST"))
        {
          con_info->postprocessor =
            MHD_create_post_processor (connection, POSTBUFFERSIZE,
                                       iterate_post, (void *) con_info);

          if (NULL == con_info->postprocessor)
            {
              free (con_info);
              return MHD_NO;
            }

          con_info->connectiontype = POST;
          con_info->answerjson = "";
          con_info->answercode = MHD_HTTP_OK;
        }
      else {
        con_info->connectiontype = GET;
      }

      *con_cls = (void *) con_info;

      return MHD_YES;
    }

  if (0 == strcmp (method, "GET"))
    {
      return send_json (connection, "{\"error\" : \"get requests are invalid\"}", MHD_HTTP_OK);
    }

  if (0 == strcmp (method, "POST"))
    {
      struct connection_info_struct *con_info = (connection_info_struct *) *con_cls;

      if (0 != *upload_data_size)
        {
          MHD_post_process (con_info->postprocessor, upload_data,
                            *upload_data_size);
          *upload_data_size = 0;

          return MHD_YES;
        }
      else
        {
          // everything is done
          if (con_info->answerjson != "") {
            return process_and_send_json (connection, con_info->answerjson,
                              con_info->answercode);
          }
          else {
            return send_json(connection, "{\"error\" : \"empty request\"}", con_info->answercode);
          }
        }
    }

  return send_json (connection, "{\"error\" : \"bad request\"}", MHD_HTTP_BAD_REQUEST);
}

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
    *buf = (char *) malloc( fsz+add_nul );
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



int
main ()
{
  struct MHD_Daemon *daemon;
  slurp("password.txt",&PASSWORD,1);


  daemon = MHD_start_daemon (MHD_USE_SELECT_INTERNALLY, PORT, NULL, NULL,
                             &answer_to_connection, NULL,
                             MHD_OPTION_NOTIFY_COMPLETED, request_completed,
                             NULL, MHD_OPTION_END);
  if (NULL == daemon) {
    return 1;
  }
  getchar ();

  MHD_stop_daemon (daemon);

  return 0;
}