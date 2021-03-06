purcell.make_websocket = function(session_name, attrs) {
  var out = attrs.out;
  var initialize = attrs.initialize;
  var cache = attrs.cache;
  var pwd = attrs.pwd;
  var ws_url = attrs.ws_url;
  console.log(out, initialize, cache, pwd);
  purcell.$e$$ion$[session_name].$0cket = new WebSocket(ws_url ? ws_url : "ws://localhost:9000", 'purcell-engraving-protocol');
  purcell.$e$$ion$[session_name].$0cket.onmessage = function(evt) {
    console.log("RECEIVED DATA", evt.data);
    json = eval("("+evt.data+")")
    var subsequent = json['subsequent'];
    console.log("subs", json['subsequent']);
    if (subsequent) {
      eval(subsequent+"("+evt.data+")");
    }
    purcell.$e$$ion$[session_name].CURRENT_DATA = evt.data;
    for (var i = 0; i < purcell.$e$$ion$[session_name].function_queue.length; i++) {
      console.log('executing', purcell.$e$$ion$[session_name].function_queue[i]);
      purcell.$e$$ion$[session_name].function_queue[i]();
    }
    purcell.$e$$ion$[session_name].function_queue = [];
  }
  purcell.$e$$ion$[session_name].$0cket.onopen = function() {
    var init =  {
           client:purcell.$e$$ion$[session_name].MY_NAME,
           initialize : initialize ? initialize : '',
           'return': 'just_me',
      }
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(
    init
    ));
    out = {
           client:purcell.$e$$ion$[session_name].MY_NAME,
           sql:out,
           'return': 'just_me',
          };
    if (cache) {
      out.cache_path = cache;
      out.password = pwd;
    }
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
}
