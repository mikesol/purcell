import output_to_sql

SQL = output_to_sql.output_to_sql()
websocket_file = file('../websocket/simple-server.c', 'r')
websocket_text = websocket_file.read()
websocket_file.close()

SQL = SQL.replace('"','\\"')
SQL = SQL.split('\n')
SQL = ['"'+sql+' "' for sql in SQL]
SQL = '\n'.join(SQL)

websocket_text = websocket_text.replace('"REPLACE_ME_WITH_VALID_SQL"',SQL)

websocket_file = file('../websocket/simple-server-with-sql.c', 'w')
websocket_file.write(websocket_text)
websocket_file.close()

