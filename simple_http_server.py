import time
import BaseHTTPServer
import json

_HOST_NAME = '' 
_PORT_NUMBER = 8000

class MyServer(BaseHTTPServer.HTTPServer) :
  def __init__(self, *args, **kwargs) :
    engraver = kwargs['engraver']
    del kwargs['engraver']
    BaseHTTPServer.HTTPServer.__init__(self, *args, **kwargs)
    self.engraver = engraver

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  def do_HEAD(s):
    s.send_response(200)
    s.send_header("Content-type", "application/json")
    s.end_headers()
  def do_GET(s):
    s.send_response(400)
  def do_POST(s) :
    clen = int(s.headers['Content-length'])
    content = s.rfile.read(clen)
    print content
    s.send_response(200)
    s.send_header("Content-type", "application/json")
    s.send_header("Access-Control-Allow-Origin","*")
    s.end_headers()
    out = s.server.engraver.engrave(content)
    s.wfile.write(out)

if __name__ == '__main__':
  class Engraver(object) :
    def engrave(self, v) :
      return v+'bar'
  server_class = MyServer
  httpd = server_class((_HOST_NAME, _PORT_NUMBER), MyHandler, engraver=Engraver())
  print time.asctime(), "Server Starts - %s:%s" % (_HOST_NAME, _PORT_NUMBER)
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    pass
  httpd.server_close()
  print time.asctime(), "Server Stops - %s:%s" % (_HOST_NAME, _PORT_NUMBER)

