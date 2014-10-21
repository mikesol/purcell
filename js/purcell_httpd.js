purcell.make_websocket = function(session_name, attrs) {
/*
  begin code dup
*/
  var out = attrs.out;
  var initialize = attrs.initialize;
  var cache = attrs.cache;
  var pwd = attrs.pwd;
  var ws_url = attrs.ws_url;
  console.log(out, initialize, cache, pwd);
/*
  end code dup
*/
  purcell.$e$$ion$[session_name].$0cket = {};
/*
  begin code dup
  evt.data changing to data_from_server
*/
  purcell.$e$$ion$[session_name].$0cket.onmessage = function(data_from_server) {
    console.log("RECEIVED DATA", data_from_server);
    json = data_from_server;
    var subsequent = json['subsequent'];
    console.log("subs", json['subsequent']);
    if (subsequent) {
      eval(subsequent+"("+JSON.stringify(data_from_server)+")");
    }
    purcell.$e$$ion$[session_name].CURRENT_DATA = data_from_server;
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
/*
  end code dup
*/
  purcell.$e$$ion$[session_name].$0cket.send = function(data) {
    $.post(ws_url, "data="+encodeURIComponent(data), purcell.$e$$ion$[session_name].$0cket.onmessage);
  }
  // call onopen
  purcell.$e$$ion$[session_name].$0cket.onopen();
}
