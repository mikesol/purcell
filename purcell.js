var purcell = {};
purcell.WS = null;
purcell.$nap = null;
purcell.GLOBAL_NOTE = 0;
purcell.GLOBAL_OCTAVE = 0;
purcell.GLOBAL_ACCIDENTAL = null;
purcell.GLOBAL_BEAM = 10000;
purcell.GLOBAL_BEAM_FLAG = false;
purcell.GLOBAL_DURATION = -2;
purcell.GLOBAL_X_SHIFT = 30;
purcell.MAX_X = 0;
purcell.shiftX = function(v) {
  if (v == null) {
    purcell.GLOBAL_X_SHIFT = 30;
  } else if (v == 3.1416) {
    // uggh
    purcell.GLOBAL_X_SHIFT = -1 * purcell.s_(purcell.MAX_X) + 400;
  } else {
    purcell.GLOBAL_X_SHIFT += v;
  }
  purcell.GLOBAL_X_SHIFT = Math.min(30, purcell.GLOBAL_X_SHIFT);
  out = []
  purcell.append_standard_graphical_queries(out);
  out = {client:purcell.MY_NAME, sql:out, 'return': purcell._be(purcell.MY_NAME), subsequent:"purcell.draw"};
  purcell.WS.send(JSON.stringify(out));
}
purcell.updateCurrentPitch = function() {
  $("#currentPitch").text("C D E F G A B R |".split(" ")[purcell.GLOBAL_NOTE]);
  $("#currentPitch").css('width',
  "40px").css("display","inline-block").css("text-align","center").css("color","red");
}
purcell.updateCurrentRhythm = function() {
  $("#currentRhythm").text(['\ue1d2','\ue1d3','\ue1d5','\ue1d7',
     '\ue1d9','\ue1db','\ue1dd','\ue1df'][purcell.GLOBAL_DURATION * -1]);
  $("#currentRhythm").css('font-family', 'Bravura').css('width',
  "20px").css("display","inline-block").css("text-align","center").css("color","red");
}
purcell.updateCurrentAccidental = function() {
  $("#currentAccidental").text(['\u25a1','\ue260','\ue261','\ue262'
     ][purcell.GLOBAL_ACCIDENTAL == null ?
     0 : purcell.GLOBAL_ACCIDENTAL + 2]);
  $("#currentAccidental").css('font-family', 'Bravura').css('width',
  "20px").css("display","inline-block").css("text-align","center").css("color","red");
}
purcell.updateCurrentOctave = function() {
  $("#currentOctave").text(purcell.GLOBAL_OCTAVE);
  $("#currentOctave").css('width',
  "40px").css("display","inline-block").css("text-align","center");
}
purcell.addNoteN = function(v) {
  if (purcell.s_(purcell.MAX_X) > 400) {
    console.log("MAX-x,",purcell.s_(purcell.MAX_X));
    purcell.GLOBAL_X_SHIFT = -1 * purcell.s_(purcell.MAX_X) + 400;
  }
  purcell.GLOBAL_NOTE = v;
  purcell.updateCurrentPitch();
  purcell.increment_and_execute("purcell.addNote_2");
  $('#spinny').spin(); // Creates a default Spinner using the text color
}
purcell.addBarLineN = function() {
  purcell.increment_and_execute("purcell.addBarLine");
  $('#spinny').spin(); // Creates a default Spinner using the text color
}
purcell.changeDuration = function(v) {
  purcell.GLOBAL_DURATION = v;
  purcell.updateCurrentRhythm();
}
purcell.changeAccidental = function(v) {
  purcell.GLOBAL_ACCIDENTAL = v;
  purcell.updateCurrentAccidental();
}
purcell.changeOctave = function(v) {
  purcell.GLOBAL_OCTAVE += v;
  purcell.updateCurrentOctave();
}
purcell.beamOn = function() {
  purcell.GLOBAL_BEAM_FLAG = true;
}
purcell.beamOff = function() {
  purcell.GLOBAL_BEAM_FLAG = false;
  purcell.GLOBAL_BEAM += 1;
}
purcell.makeid = function() {
  var text = "";
  var possible =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  for( var i=0; i < 8; i++ ) {
    text += possible.charAt(Math.floor(Math.random() *
    possible.length));
  }

  return text;
}
purcell._be = function(s) {
  return "^"+s+"$"
}
purcell.s_ = function(v) {
  return 4.0 * v;
}
purcell.st_x = function(v) {
  return purcell.s_(v) + purcell.GLOBAL_X_SHIFT;
}
purcell.st_y = function(v) {
  return purcell.s_(v) + 30;
}
purcell.build_simple_insert = function(table, dct) {
  //console.log(table, dct);
  var keys = []
  for (var key in dct) {
    keys.push(key)
  }
  var stmt = "INSERT INTO "+table+" ("
  stmt += keys.join();
  stmt += ") VALUES ("
  var vals = [];
  for (var i = 0 ; i < keys.length; i++) {
    vals.push(dct[keys[i]]);
  }
  stmt += vals.join();
  stmt += ");"
  return stmt;
}
purcell.increment_last_used_item = function() {
    return "INSERT INTO used_ids (id) SELECT max(used_ids.id) + 1 FROM used_ids;";
}
purcell.get_last_used_item = function() {
    return "SELECT max(used_ids.id) FROM used_ids;";
}
purcell.increment_and_execute = function(subsequent) {
  var out = [];
  out.push({
    expected : [],
    sql : purcell.increment_last_used_item()
  });
  out.push({
    name : 'next',
    expected : ['id'],
    sql : purcell.get_last_used_item()
  });
  out.push({
    name : 'prev',
    expected : ['id'],
    sql : "SELECT graphical_next.id FROM graphical_next WHERE graphical_next.next IS NULL;"
  });
  out.push({
    name : 'prev_prev',
    expected : ['id'],
    sql : "SELECT graphical_next.prev FROM graphical_next WHERE graphical_next.next IS NULL;"
  });
  out = {client:purcell.MY_NAME, sql:out, 'return': purcell._be(purcell.MY_NAME), subsequent: subsequent};
  purcell.WS.send(JSON.stringify(out));
}
purcell.addNote_2 = function(data) {
  console.log("data going to AN_2", data);
  out = [];
  var prev = data['prev'][0]['id'];
  var prev_prev = data['prev_prev'][0]['id'];
  var next = data['next'][0]['id'];
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('name',{id:next, val :
    purcell.GLOBAL_NOTE == null ? "'rest'" : "'note'"})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('font_name',{id:next,
    val : "'Bravura'"})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('font_size',{id:next, val : 20})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('duration_log',{id:next,
    // hmmm...
    val : purcell.GLOBAL_DURATION})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('staff_symbol',{id:next,
    val : 1})
  });
  if (purcell.GLOBAL_NOTE != null) {
    if (purcell.GLOBAL_ACCIDENTAL != null) {
      out.push({
        expected : [],
        sql : purcell.build_simple_insert('accidental',{id:next,
        val : purcell.GLOBAL_ACCIDENTAL})
      });
    }
    out.push({
      expected : [],
      sql : purcell.build_simple_insert('pitch',{id:next,
      val : purcell.GLOBAL_NOTE})
    });
    out.push({
      expected : [],
      sql : purcell.build_simple_insert('octave',{id:next,
      val : purcell.GLOBAL_OCTAVE})
    });
    // testing for beams!
    if (purcell.GLOBAL_BEAM_FLAG) {
      out.push({
        expected : [],
        sql : purcell.build_simple_insert('beam',{id:next,
        // ugh, for now just 5000...
        val : purcell.GLOBAL_BEAM})
      });
    }
  }
  out.push({
    expected : [],
    sql : "DELETE FROM graphical_next WHERE graphical_next.id = "+prev+";" 
  });
  out.push({
    expected : [],
    sql: "INSERT INTO graphical_next (id, prev, next) VALUES("+prev+","+prev_prev+","+next+");"
  });
  out.push({
    expected : [],
    sql: "INSERT INTO graphical_next (id, prev, next) VALUES("+next+","+prev+",NULL);"
  });
  purcell.append_standard_graphical_queries(out);
  out = {client:purcell.MY_NAME, sql:out, 'return': "*", subsequent: "purcell.draw"};
  ///////////////////////////
  purcell.WS.send(JSON.stringify(out));
}
purcell.addBarLine = function(data) {
  console.log("data going to adBarLine", data);
  out = [];
  var prev = data['prev'][0]['id'];
  var prev_prev = data['prev_prev'][0]['id'];
  var next = data['next'][0]['id'];
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('name',{id:next, val :
    "'bar_line'"})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('bar_thickness',{id:next, val : 0.20})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('staff_symbol',{id:next,
    val : 1})
  });
  out.push({
    expected : [],
    sql : "DELETE FROM graphical_next WHERE graphical_next.id = "+prev+";" 
  });
  out.push({
    expected : [],
    sql: "INSERT INTO graphical_next (id, prev, next) VALUES("+prev+","+prev_prev+","+next+");"
  });
  out.push({
    expected : [],
    sql: "INSERT INTO graphical_next (id, prev, next) VALUES("+next+","+prev+",NULL);"
  });
  purcell.append_standard_graphical_queries(out);
  out = {client:purcell.MY_NAME, sql:out, 'return': "*", subsequent: "purcell.draw"};
  ///////////////////////////
  purcell.WS.send(JSON.stringify(out));
}
purcell.table_to_columns = function(name) {
  if (name == "line_stencil") {
    return "line_stencil.id, line_stencil.x0, line_stencil.y0, line_stencil.x1, line_stencil.y1, line_stencil.thickness";
  } else if (name == "glyph_stencil") {
    return "glyph_stencil.id, glyph_stencil.font_name, glyph_stencil.font_size, glyph_stencil.unicode, glyph_stencil.x, glyph_stencil.y";
  } else if (name == "polygon_stencil") {
    return "polygon_stencil.id, polygon_stencil.sub_id, polygon_stencil.point, polygon_stencil.x, polygon_stencil.y, polygon_stencil.thickness, polygon_stencil.stroke, polygon_stencil.fill";
  }
}
purcell.stencil_sql_request = function(name) {
  var out = "SELECT " + purcell.table_to_columns(name) + ", x_position.val, y_position.val FROM " + name + " LEFT JOIN x_position ON " + name + ".id = x_position.id LEFT JOIN y_position ON " + name + ".id = y_position.id";
  return out + ";"
}
purcell.append_standard_graphical_queries = function(out) {
  out.push({
    name : 'line_stencil',
    expected : ['id', 'x0', 'y0',
    'x1', 'y1', 'thickness',
    'x_position', 'y_position'],
    sql : purcell.stencil_sql_request('line_stencil')
  });
  out.push({
    name : 'glyph_stencil',
    expected : ['id', 'font_name', 'font_size',
    'unicode', 'x', 'y',
    'x_position', 'y_position'],
    sql : purcell.stencil_sql_request('glyph_stencil')
  });
  out.push({
    name : 'polygon_stencil',
    expected : ['id', 'sub_id', 'point',
    'x', 'y',
    'thickness', 'stroke', 'fill',
    'x_position','y_position'],
    sql : purcell.stencil_sql_request('polygon_stencil')
  });
  out.push({
    name : 'max_x',
    expected : ['x'],
    sql : "SELECT max(x_position.val) FROM x_position;"
  });
}
purcell.draw = function(data) {
  console.log("data going to draw", data)
  purcell.MAX_X = data.max_x[0]['x'];
  purcell.$nap.clear();
  _gr0up = purcell.$nap.group().attr({transform:'scale(2,2);'});
  for (var i = 0; i < data.line_stencil.length; i++) {
    var line = data.line_stencil[i];
    console.log("making line", line);
    var x = line['x_position'] ? line['x_position'] : 0.0;
    var y = line['y_position'] ? line['y_position'] : 0.0;
    _gr0up.line(purcell.st_x(line['x0'] + x), purcell.st_y(line['y0'] + y),
               purcell.st_x(line['x1'] + x), purcell.st_y(line['y1'] + y)).attr({
       stroke : 'black',
       strokeWidth : purcell.s_(line['thickness'])
    });
  }        
  for (var i = 0; i < data.glyph_stencil.length; i++) {
    var glyph = data.glyph_stencil[i];
    var x = glyph['x'] + (glyph['x_position'] ? glyph['x_position'] : 0.0);
    var y = glyph['y'] + (glyph['y_position'] ? glyph['y_position'] : 0.0);
    uc = String.fromCharCode(parseInt(glyph['unicode'].substr(2), 16));
    //console.log(context.font, glyph[3], uc);
    _gr0up.text(purcell.st_x(x), purcell.st_y(y), uc).attr({
      "font-family" : glyph['font_name'],
      "font-size" : glyph['font_size']
    });
  }
  $('#spinny').spin(false); // Stops and removes the spinner.
}
purcell.initialize = function() {
  purcell.MY_NAME = purcell.makeid();
  purcell.WS = new WebSocket("ws://localhost:8000");
  purcell.$nap = Snap("#engraving");
  _gr0up = purcell.$nap.group();

  purcell.WS.onopen = function() {
    out = []
    purcell.append_standard_graphical_queries(out);
    out = {client:purcell.MY_NAME, sql:out, 'return': purcell._be(purcell.MY_NAME), subsequent:"purcell.draw"};
    purcell.WS.send(JSON.stringify(out));
  }
  purcell.WS.onmessage = function(evt) {
    //console.log(evt.data);
    //var json = eval("("+evt.data+")");
    //console.log(json);
    json = eval("("+evt.data+")")
    var subsequent = json['subsequent'];
    console.log("subs", json['subsequent']);
    if (subsequent) {
      eval(subsequent+"("+evt.data+")");
    }
  }
  purcell.updateCurrentPitch();
  purcell.updateCurrentRhythm();
  purcell.updateCurrentAccidental();
  purcell.updateCurrentOctave();
}
