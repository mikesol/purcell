purcell.make_websocket = function(session_name, out) {
  purcell.$e$$ion$[session_name].$0cket = new WebSocket("ws://localhost:9000", 'purcell-engraving-protocol');
  purcell.$e$$ion$[session_name].$0cket.onopen = function() {
    out = {
           client:purcell.$e$$ion$[session_name].MY_NAME,
           initializing:true,
           sql:out,
           'return': 'just_me',
          };
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
    out = [];
    purcell.append_standard_graphical_queries(out);
    out = {
           client:purcell.$e$$ion$[session_name].MY_NAME,
           sql:out,
           'return': 'everyone',
           subsequent:"purcell.$e$$ion$."+session_name+".draw"
          };
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }
  purcell.$e$$ion$[session_name].$0cket.onmessage = function(evt) {
    json = eval("("+evt.data+")")
    var subsequent = json['subsequent'];
    console.log("subs", json['subsequent']);
    if (subsequent) {
      eval(subsequent+"("+evt.data+")");
    }
    purcell.$e$$ion$[session_name].CURRENT_DATA = evt.data;
    for (var i = 0; i < purcell.$e$$ion$[session_name].function_queue.length; i++) {
      purcell.$e$$ion$[session_name].function_queue[i]();
    }
    purcell.$e$$ion$[session_name].function_queue = [];
  }
}
