# Purcelll

purcell implements a relational model of music engraving, using SQL queries to
insert, update, delete and select elements of a musical score.

# Dependencies for pure SQL

1. python 2.7.0 or higher
2. sqlalchemy 1.0 (development version - http://www.sqlalchemy.org)
3. sqlite 3.8.4 or higher (http://www.sqlite.org)

From the python directory, run output_to_sql.py.
The SQL of the program will be in build/raw_sql.sql

# Dependencies to create a websocket test

First, if you're going to make a websocket test, do everything above
to make the pure SQL.  Then

1. cmake 2.8 or higher
2. A garden-variety C compiler
3. libwebsockets 1.3 or higher (http://libwebsockets.org/)
4. jansson 4.6.0 or higher (newer the better: http://www.digip.org/jansson/)

# Running the websocket test

Then, from the cmake directory, do:

```
cmake -G "Unix Makefiles"
make
```

And an application called simple_server will be built in the `cmake` directory.

Run this application and a server will serve until you kill it.
While it's running, open the page at html/index.html in a modern browser.
Click on note names to make them appear.
Open it several times to edit the score from several windows.
