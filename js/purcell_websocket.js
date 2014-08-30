purcell.make_websocket = function(out) {
  purcell.$0cket = new WebSocket("ws://guido.grame.fr:9000", 'purcell-engraving-protocol');
  purcell.$0cket.onopen = function() {
    out = {
           client:purcell.MY_NAME,
           initializing:true,
           sql:out,
           'return': 'just_me',
          };
    purcell.$0cket.send(JSON.stringify(out));
    out = [];
    purcell.append_standard_graphical_queries(out);
    out = {
           client:purcell.MY_NAME,
           sql:out,
           'return': 'everyone',
           subsequent:"purcell.draw"
          };
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
