var purcell = {};
purcell.$0cket = null;
purcell.$nap = null;
purcell.GLOBAL_NOTE = 0;
purcell.GLOBAL_OCTAVE = 0;
purcell.GLOBAL_ACCIDENTAL = null;
purcell.GLOBAL_BEAM = null;
purcell.GLOBAL_BEAM_FLAG = false;
purcell.GLOBAL_DYNAMIC = null;
purcell.GLOBAL_DURATION = -2;
purcell.GLOBAL_X_SHIFT = 30;
purcell.MAX_X = 0;
purcell.CURRENT_SELECTED_OBJECT = null;
purcell.is_null = function(v) {
  if (v==null) {return true;}
  if (v=="NULL") {return true;}
  return false;
}
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
  out = {client:purcell.MY_NAME, sql:out, 'return': 'just_me', subsequent:"purcell.draw"};
  purcell.$0cket.send(JSON.stringify(out));
}
purcell.updateCurrentPitch = function() {
  $("#currentPitch").text("C D E F G A B R |".split(" ")[purcell.GLOBAL_NOTE]);
  $("#currentPitch").css('width', "40px").
       css("display","inline-block").
       css("text-align","center").
       css("color","red");
}
purcell.updateCurrentRhythm = function() {
  $("#currentRhythm").text(['\ue1d2','\ue1d3','\ue1d5','\ue1d7',
                            '\ue1d9','\ue1db','\ue1dd','\ue1df'][purcell.GLOBAL_DURATION * -1]);
  $("#currentRhythm").css('font-family', 'Bravura')
      .css('width', "20px")
      .css("display","inline-block")
      .css("text-align","center")
      .css("color","red");
}
purcell.updateCurrentAccidental = function() {
  $("#currentAccidental").text(['\u2205','\ue260','\ue261','\ue262'
     ][purcell.GLOBAL_ACCIDENTAL == null ?
     0 : purcell.GLOBAL_ACCIDENTAL + 2]);
  $("#currentAccidental").css('font-family', 'Bravura')
      .css('width', "20px")
      .css("display","inline-block")
      .css("text-align","center")
      .css("color","red");
}
purcell.updateCurrentOctave = function() {
  $("#currentOctave").text(purcell.GLOBAL_OCTAVE);
  $("#currentOctave").css('width', "40px")
       .css("display","inline-block")
       .css("text-align","center")
       .css("color","red");
}
purcell.updateCurrentBeam = function() {
  $("#currentBeam").text(purcell.GLOBAL_BEAM_FLAG ? "ON" : "OFF");
  $("#currentBeam").css('width', "80px")
      .css("display","inline-block")
      .css("text-align","center")
      .css("color","red");
}
purcell.updateCurrentDynamic = function() {
  console.log("GD", purcell.GLOBAL_DYNAMIC);
  $("#currentDynamic").text({
    null : '\u2205',
    'pppppp': "\uE527",
    'ppppp': "\uE528",
    'pppp': "\uE529",
    'ppp': "\uE52A",
    'pp': "\uE52B",
    'p': "\uE520",
    'mp': "\uE52C",
    'mf': "\uE52D",
    'p': "\uE520",
    'pf': "\uE52E",
    'f': "\uE522",
    'ff': "\uE52F",
    'fff': "\uE530",
    'ffff': "\uE531",
    'fffff': "\uE532",
    'ffffff': "\uE533",
    'fp': "\uE534",
    'fz': "\uE535",
    'sf': "\uE536",
    'sfp': "\uE537",
    'sfpp': "\uE538",
    'sfz': "\uE539"
     }[purcell.GLOBAL_DYNAMIC]);
  $("#currentDynamic").css('font-family', 'Bravura')
      .css('width', "20px")
      .css("display","inline-block")
      .css("text-align","center")
      .css("color","red");
}
purcell.addNoteN = function(v) {
  if (purcell.s_(purcell.MAX_X) > 400) {
    console.log("MAX-x,",purcell.s_(purcell.MAX_X));
    purcell.GLOBAL_X_SHIFT = -1 * purcell.s_(purcell.MAX_X) + 400;
  }
  purcell.GLOBAL_NOTE = v;
  purcell.updateCurrentPitch();
  var out = [];
  if (purcell.GLOBAL_DYNAMIC != null) {
    out = purcell.dynamic_increment(out);
  }
  $('#spinny').spin(); // Creates a default Spinner using the text color
  purcell.increment_and_execute("purcell.addNote_2",out);
}
purcell.addBarLineN = function() {
  $('#spinny').spin(); // Creates a default Spinner using the text color
  purcell.increment_and_execute("purcell.addBarLine");
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
purcell.changeDynamic= function(v) {
  purcell.GLOBAL_DYNAMIC = v;
  purcell.updateCurrentDynamic();
}
purcell.beamOn = function() {
  purcell.GLOBAL_BEAM_FLAG = true;
  purcell.updateCurrentBeam();
  if (!purcell.GLOBAL_BEAM) {
    out = [];
    out.push({
      expected : [],
      sql : purcell.increment_last_used_item()
    });
    out.push({
      name : 'beam',
      expected : ['id'],
      sql : purcell.get_last_used_item()
    });
  }
  out = {client:purcell.MY_NAME, sql:out, 'return': 'just_me', subsequent:"purcell.registerBeam"};
  purcell.$0cket.send(JSON.stringify(out));
}
purcell.registerBeam = function(data) {
  console.log("REG BEAM", data);
  purcell.GLOBAL_BEAM = data.beam[0]['id'];
}
purcell.beamOff = function() {
  purcell.GLOBAL_BEAM_FLAG = false;
  purcell.GLOBAL_BEAM = null;
  purcell.updateCurrentBeam();
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
    return "SELECT max(used_ids.id) AS id FROM used_ids;";
}

purcell.dynamic_increment = function(out) {
  out = out != null ? out : [];
  out.push({
    //expected : [],
    sql : purcell.increment_last_used_item()
  });
  out.push({
    name : 'dynamic_id',
    //expected : ['id'],
    sql : purcell.get_last_used_item()
  });
  return out;
}

purcell.increment_and_execute = function(subsequent, out) {
  console.log('IEX', out);
  out = out != null ? out : [];
  out.push({
    //expected : [],
    sql : purcell.increment_last_used_item()
  });
  out.push({
    name : 'next',
    //expected : ['id'],
    sql : purcell.get_last_used_item()
  });
  
  out.push({
    name : 'prev',
    //expected : ['id'],
    sql : "SELECT graphical_next.id AS id FROM graphical_next WHERE graphical_next.next IS NULL;"
  });
  out.push({
    name : 'prev_prev',
    //expected : ['id'],
    sql : "SELECT graphical_next.prev AS id FROM graphical_next WHERE graphical_next.next IS NULL;"
  });
  console.log("AAHHHHHH", out);
  out = {client:purcell.MY_NAME, sql:out, 'return': 'just_me', subsequent: subsequent};
  purcell.$0cket.send(JSON.stringify(out));
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
  console.log("checking data for dynamic id", data);
  if (data.dynamic_id != null) {
    out = purcell.add_dynamic(data.dynamic_id[0]['id'], next, out);
  }
  purcell.append_standard_graphical_queries(out);
  out = {client:purcell.MY_NAME, sql:out, 'return': "everyone", subsequent: "purcell.draw"};
  ///////////////////////////
  purcell.$0cket.send(JSON.stringify(out));
}
purcell.add_dynamic = function(dynamic_id, note_id, out) {
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('font_name',{id:dynamic_id,
    val : "'Bravura'"})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('font_size',{id:dynamic_id, val : 20})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('anchor_x',{id:dynamic_id,
    // hmmm...
    val : note_id})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('dynamic',{id:dynamic_id,
    // hmmm...
    val : "'"+purcell.GLOBAL_DYNAMIC+"'"})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('dynamic_direction',{id:dynamic_id,
    // hmmm...
    val : -1})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('x_position',{id:dynamic_id,
    val : 0.0})
  });
  out.push({
    expected : [],
    sql : purcell.build_simple_insert('staff_symbol',{id:dynamic_id,
    val : 1})
  });
  return out;
}
purcell.addBarLine = function(data) {
  console.log("data going to addBarLine", data);
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
  out = {client:purcell.MY_NAME, sql:out, 'return': "everyone", subsequent: "purcell.draw"};
  ///////////////////////////
  purcell.$0cket.send(JSON.stringify(out));
}
purcell.table_to_columns = function(name) {
  if (name == "line_stencil") {
    return "line_stencil.id AS id, line_stencil.sub_id AS sub_id, line_stencil.x0 AS x0, line_stencil.y0 AS y0, line_stencil.x1 AS x1, line_stencil.y1 AS y1, line_stencil.thickness AS thickness";
  } else if (name == "glyph_stencil") {
    return "glyph_stencil.id AS id, glyph_stencil.sub_id AS sub_id, glyph_stencil.font_name AS font_name, glyph_stencil.font_size AS font_size, glyph_stencil.unicode AS unicode, glyph_stencil.x AS x, glyph_stencil.y AS y";
  } else if (name == "polygon_stencil") {
    return "polygon_stencil.id AS id, polygon_stencil.sub_id AS sub_id, polygon_stencil.point AS point, polygon_stencil.x AS x, polygon_stencil.y AS y, polygon_stencil.thickness AS thickness, polygon_stencil.stroke AS stroke, polygon_stencil.fill AS fill";
  }
}
purcell.stencil_sql_request = function(name) {
  var out = "SELECT " + purcell.table_to_columns(name) + ", anchored_x_position.val AS x_position, anchored_y_position.val AS y_position, name.val FROM " + name + " LEFT JOIN anchored_x_position ON " + name + ".id = anchored_x_position.id LEFT JOIN anchored_y_position ON " + name + ".id = anchored_y_position.id LEFT JOIN name ON " + name + ".id = name.id";
  return out + ";";
}
purcell.append_standard_graphical_queries = function(out) {
  out.push({
    name : 'line_stencil',
    /*expected : ['id', 'sub_id', 'x0', 'y0',
    'x1', 'y1', 'thickness',
    'x_position', 'y_position','name'],*/
    sql : purcell.stencil_sql_request('line_stencil')
  });
  out.push({
    name : 'glyph_stencil',
    /*expected : ['id', 'sub_id', 'font_name', 'font_size',
    'unicode', 'x', 'y',
    'x_position', 'y_position','name'],*/
    sql : purcell.stencil_sql_request('glyph_stencil')
  });
  out.push({
    name : 'polygon_stencil',
    /*expected : ['id', 'sub_id', 'point',
    'x', 'y',
    'thickness', 'stroke', 'fill',
    'x_position','y_position','name'],*/
    sql : purcell.stencil_sql_request('polygon_stencil')
  });
  out.push({
    name : 'max_x',
    //expected : ['x'],
    sql : "SELECT max(anchored_x_position.val) AS x FROM anchored_x_position;"
  });
}

purcell.makeCurrentSelectedObjectColor = function(c) {
  var elt = purcell.$nap.select('#'+purcell.CURRENT_SELECTED_OBJECT);
  if (elt.attr('fill') != 'none') {
    elt.attr({fill : c})
  }
  if (elt.attr('stroke') != 'none') {
    console.log(elt.attr('stroke'))
    elt.attr({stroke : c})
  }  
}
purcell.registerAsClicked = function(id) {
  //console.log("SOMETHING",elt, $(elt).attr('id'));
  if (purcell.CURRENT_SELECTED_OBJECT) {
    purcell.makeCurrentSelectedObjectColor('black');
  }
  purcell.CURRENT_SELECTED_OBJECT = id;
  purcell.makeCurrentSelectedObjectColor('red');
}
purcell.draw = function(data) {
  console.log("data going to draw", data)
  purcell.MAX_X = data.max_x[0]['x'];
  purcell.$nap.clear();
  _gr0up = purcell.$nap.group().attr({transform:'scale(2,2);'});
  if (data.line_stencil != null) {
    for (var i = 0; i < data.line_stencil.length; i++) {
      var line = data.line_stencil[i];
      console.log("making line", line);
      var x = parseFloat(!purcell.is_null(line['x_position']) ? line['x_position'] : 0.0);
      var y = parseFloat(!purcell.is_null(line['y_position']) ? line['y_position'] : 0.0);
      var name = line['name'] ? line['name'] : '';
      var s_line = _gr0up.line(purcell.st_x(parseFloat(line['x0']) + x),
                               purcell.st_y(parseFloat(line['y0']) + y),
                               purcell.st_x(parseFloat(line['x1']) + x),
                               purcell.st_y(parseFloat(line['y1']) + y)).attr({
         stroke : 'black',
         strokeWidth : purcell.s_(parseFloat(line['thickness'])),
         'id' : name+'_line_'+line['id']+'_'+line['sub_id'],
      });
      s_line.click(function() { return function() { purcell.registerAsClicked(s_line) } });
    }
  }
  if (data.glyph_stencil != null) {
    for (var i = 0; i < data.glyph_stencil.length; i++) {
      var closure = function () {
        var glyph = data.glyph_stencil[i];
        console.log("making glyph", glyph);
        var x = parseFloat(glyph['x']) + parseFloat(!purcell.is_null(glyph['x_position']) ? glyph['x_position'] : 0.0);
        var y = parseFloat(glyph['y']) + parseFloat(!purcell.is_null(glyph['y_position']) ? glyph['y_position'] : 0.0);
        uc = String.fromCharCode(parseInt(glyph['unicode'].substr(2), 16));
        var name = glyph['name'] ? glyph['name'] : '';
        //console.log(context.font, glyph[3], uc);
        var s_glyph = _gr0up.text(purcell.st_x(x), purcell.st_y(y), uc).attr({
          "font-family" : glyph['font_name'],
          "font-size" : glyph['font_size'],
           'id' : name+'_glyph_'+glyph['id']+'_'+glyph['sub_id'],
        });
        s_glyph.click(function() { purcell.registerAsClicked(s_glyph.attr('id'))  } );
      };
      closure();
    }
  }
  var polygon_holder = {};
  if (data.polygon_stencil != null) {
    for (var i = 0; i < data.polygon_stencil.length; i++) {
      var polygon = data.polygon_stencil[i];
      console.log("PG",polygon);
      if (!polygon_holder[polygon['id']]) {
        polygon_holder[polygon['id']] = {};
      }
      if (!polygon_holder[polygon['id']][polygon['sub_id']]) {
        console.log("making sub", polygon['sub_id']);
        polygon_holder[polygon['id']][polygon['sub_id']] = [];
      }
      polygon_holder[polygon['id']][polygon['sub_id']].push(polygon);
    }
    console.log("PH", polygon_holder);
    for (key in polygon_holder) {
      for (sub_key in polygon_holder[key]) {
        // first, we sort in point order
        console.log("before sorting", polygon_holder[key][sub_key])
        polygon_holder[key][sub_key].sort(function(a,b)
          {
            return parseInt(a['point']) - parseInt(b['point']);
          });
        console.log("after sorting", polygon_holder[key][sub_key])
        // then, iterate
        var path = "";
        for (var i = 0; i < polygon_holder[key][sub_key].length; i++) {
          path = path + ((i == 0 ? 'M' : 'L') + " " + purcell.st_x(parseFloat(polygon_holder[key][sub_key][i].x)) + " " + purcell.st_y(parseFloat(polygon_holder[key][sub_key][i].y))+ " ");
        }
        var name = polygon_holder[key][sub_key][0]['name'] ? polygon_holder[key][sub_key][0]['name'] : '';
        var thick = parseFloat(polygon_holder[key][sub_key][0].thickness ? polygon_holder[key][sub_key][0].thickness : 0.0); 
        var s_polygon = _gr0up.path(path).attr({
          fill : polygon_holder[key][sub_key][0].fill == 1 ? true : false,
          stroke : 'black',
          strokeWidth : parseFloat(polygon_holder[key][sub_key][0].stroke) * thick,
         'id' : name+'_polygon_'+polygon['id']+'_'+polygon['sub_id'],
        }).click(purcell.registerAsClicked);
        s_polygon.click(function() { return function() { purcell.registerAsClicked(s_polygon) } });
      }
      
    }
  }
  console.log("CSO", purcell.CURRENT_SELECTED_OBJECT);
    if (purcell.CURRENT_SELECTED_OBJECT) {
      purcell.makeCurrentSelectedObjectColor('red');
  }
  $('#spinny').spin(false); // Stops and removes the spinner.
}
purcell.initialize = function() {
  purcell.MY_NAME = purcell.makeid();
  purcell.$nap = Snap("#engraving");
  _gr0up = purcell.$nap.group();

  purcell.updateCurrentPitch();
  purcell.updateCurrentRhythm();
  purcell.updateCurrentAccidental();
  purcell.updateCurrentOctave();
  purcell.updateCurrentBeam();
  purcell.updateCurrentDynamic();
}
