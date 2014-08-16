This is, for now, a hack to show that SQL can be used to typeset music
and retrieve data about it.

DEPENDENCIES

A garden-variety C compiler
cmake 2.8 or higher
python 2.7.0 or higher
sqlalchemy 1.0 (development version - http://www.sqlalchemy.org)
sqlite 3.8.4 or higher (http://www.sqlite.org)
libwebsockets 1.3 or higher (http://libwebsockets.org/)
jansson 4.6.0 or higher (newer the better: http://www.digip.org/jansson/)

Then, from the cmake directory, do:

cmake -G "Unix Makefiles"
make

And an application called simple server will be there.
Run this application and then check out the presentation at html/index.html
(slide #7, then down 1).