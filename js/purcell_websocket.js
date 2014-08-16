purcell.make_websocket = function(out) {
  out = out != null ? out : [];
  purcell.$0cket = new WebSocket("ws://localhost:9000", 'dumb-increment-protocol');
  purcell.$0cket.onopen = function() {
    purcell.append_standard_graphical_queries(out);
    out = {client:purcell.MY_NAME, sql:out, 'return': purcell._be(purcell.MY_NAME), subsequent:"purcell.draw"};
    purcell.$0cket.send(JSON.stringify(out));
  }
  purcell.$0cket.onmessage = function(evt) {
    json = eval("("+evt.data+")")
    var subsequent = json['subsequent'];
    console.log("subs", json['subsequent']);
    if (subsequent) {
      eval(subsequent+"("+evt.data+")");
    }
    purcell.CURRENT_DATA = evt.data;
  }
}
