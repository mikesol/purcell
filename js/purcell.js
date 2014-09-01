// global to purcell
var purcell = {};
purcell.is_null = function(v) {
  if (v==null) {return true;}
  if (v=="NULL") {return true;}
  return false;
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
purcell.updateCurrentPitch = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentPitch();
}
purcell.updateCurrentRhythm = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentRhythm();
}
purcell.updateCurrentAccidental = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentAccidental();
}
purcell.updateCurrentOctave = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentOctave();
}
purcell.updateCurrentBeam = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentBeam();
}
purcell.updateCurrentDynamic = function(session_name) {
  purcell.$e$$ion$[session_name].updateCurrentDynamic();
}


// session specific
purcell.$e$$ion$ = {};
purcell.make_session = function(session_name, ids) {
  purcell.$e$$ion$[session_name] = {};

  purcell.$e$$ion$[session_name].$0cket = null;
  purcell.$e$$ion$[session_name].$urfaces = [];
  purcell.$e$$ion$[session_name].function_queue = [];
  purcell.$e$$ion$[session_name].GLOBAL_NOTE = 0;
  purcell.$e$$ion$[session_name].GLOBAL_OCTAVE = 0;
  purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL = null;
  purcell.$e$$ion$[session_name].GLOBAL_BEAM = null;
  purcell.$e$$ion$[session_name].GLOBAL_BEAM_FLAG = false;
  purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC = null;
  purcell.$e$$ion$[session_name].GLOBAL_DURATION = -2;
  purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT = 30;
  purcell.$e$$ion$[session_name].MAX_X = 0;
  purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT = null;

  contexts = ids ? ids : ['engraving'];
  purcell.$e$$ion$[session_name].MY_NAME = purcell.makeid();
  for (var surface_index = 0; surface_index < ids.length; surface_index++) {
    purcell.$e$$ion$[session_name].$urfaces[surface_index] = Snap("#"+ids[surface_index]);
  }


  purcell.$e$$ion$[session_name].shiftX = function(v) {
    if (v == null) {
      purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT = 30;
    } else if (v == 3.1416) {
      // uggh
      purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT = -1 * purcell.$e$$ion$[session_name].s_(purcell.$e$$ion$[session_name].MAX_X) + 400;
    } else {
      purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT += v;
    }
    purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT = Math.min(30, purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT);
    out = []
    purcell.append_standard_graphical_queries(out);
    out = {client:purcell.$e$$ion$[session_name].MY_NAME, sql:out, 'return': 'just_me', subsequent:"purcell.$e$$ion$."+session_name+".draw"};
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }

  purcell.$e$$ion$[session_name].updateCurrentPitch = function() {
    $("#"+session_name+"_currentPitch").text("C D E F G A B R |".split(" ")[purcell.$e$$ion$[session_name].GLOBAL_NOTE]);
    $("#"+session_name+"_currentPitch").css('width', "40px").
         css("display","inline-block").
         css("text-align","center").
         css("color","red");
  }
  purcell.$e$$ion$[session_name].updateCurrentRhythm = function() {
    $("#"+session_name+"_currentRhythm").text(['\ue1d2','\ue1d3','\ue1d5','\ue1d7',
                              '\ue1d9','\ue1db','\ue1dd','\ue1df'][purcell.$e$$ion$[session_name].GLOBAL_DURATION * -1]);
    $("#"+session_name+"_currentRhythm").css('font-family', 'Bravura')
        .css('width', "20px")
        .css("display","inline-block")
        .css("text-align","center")
        .css("color","red");
  }
  purcell.$e$$ion$[session_name].updateCurrentAccidental = function() {
    $("#"+session_name+"_currentAccidental").text(['\u2205','\ue260','\ue261','\ue262'
       ][purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL == null ?
       0 : purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL + 2]);
    $("#"+session_name+"_currentAccidental").css('font-family', 'Bravura')
        .css('width', "20px")
        .css("display","inline-block")
        .css("text-align","center")
        .css("color","red");
  }
  purcell.$e$$ion$[session_name].updateCurrentOctave = function() {
    $("#"+session_name+"_currentOctave").text(purcell.$e$$ion$[session_name].GLOBAL_OCTAVE);
    $("#"+session_name+"_currentOctave").css('width', "40px")
         .css("display","inline-block")
         .css("text-align","center")
         .css("color","red");
  }
  purcell.$e$$ion$[session_name].updateCurrentBeam = function() {
    $("#"+session_name+"_currentBeam").text(purcell.$e$$ion$[session_name].GLOBAL_BEAM_FLAG ? "ON" : "OFF");
    $("#"+session_name+"_currentBeam").css('width', "80px")
        .css("display","inline-block")
        .css("text-align","center")
        .css("color","red");
  }
  purcell.$e$$ion$[session_name].updateCurrentDynamic = function() {
    //console.log("GD", purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC);
    $("#"+session_name+"_currentDynamic").text({
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
       }[purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC]);
    $("#"+session_name+"_currentDynamic").css('font-family', 'Bravura')
        .css('width', "20px")
        .css("display","inline-block")
        .css("text-align","center")
        .css("color","red");
  }
  purcell.$e$$ion$[session_name].addNoteN = function(v) {
    if (purcell.$e$$ion$[session_name].s_(purcell.$e$$ion$[session_name].MAX_X) > 400) {
      //console.log("MAX-x,",purcell.$e$$ion$[session_name].s_(purcell.$e$$ion$[session_name].MAX_X));
      purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT = -1 * purcell.$e$$ion$[session_name].s_(purcell.$e$$ion$[session_name].MAX_X) + 400;
    }
    purcell.$e$$ion$[session_name].GLOBAL_NOTE = v;
    purcell.$e$$ion$[session_name].updateCurrentPitch();
    var out = [];
    if (purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC != null) {
      out = purcell.dynamic_increment(out);
    }
    $('#'+session_name+'_spinny').spin(); // Creates a default Spinner using the text color
    purcell.$e$$ion$[session_name].increment_and_execute("purcell.$e$$ion$."+session_name+".addNote_2",out);
  }
  purcell.$e$$ion$[session_name].addBarLineN = function() {
    $('#'+session_name+'_spinny').spin(); // Creates a default Spinner using the text color
    purcell.$e$$ion$[session_name].increment_and_execute("purcell.$e$$ion$."+session_name+".addBarLine");
  }
  purcell.$e$$ion$[session_name].changeDuration = function(v) {
    purcell.$e$$ion$[session_name].GLOBAL_DURATION = v;
    purcell.$e$$ion$[session_name].updateCurrentRhythm();
  }
  purcell.$e$$ion$[session_name].changeAccidental = function(v) {
    purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL = v;
    purcell.$e$$ion$[session_name].updateCurrentAccidental();
  }
  purcell.$e$$ion$[session_name].changeOctave = function(v) {
    purcell.$e$$ion$[session_name].GLOBAL_OCTAVE += v;
    purcell.$e$$ion$[session_name].updateCurrentOctave();
  }
  purcell.$e$$ion$[session_name].changeDynamic= function(v) {
    purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC = v;
    purcell.$e$$ion$[session_name].updateCurrentDynamic();
  }
  purcell.$e$$ion$[session_name].beamOn = function() {
    purcell.$e$$ion$[session_name].GLOBAL_BEAM_FLAG = true;
    purcell.$e$$ion$[session_name].updateCurrentBeam();
    if (!purcell.$e$$ion$[session_name].GLOBAL_BEAM) {
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
    out = {client:purcell.$e$$ion$[session_name].MY_NAME, sql:out, 'return': 'just_me', subsequent:"purcell.$e$$ion$."+session_name+".registerBeam"};
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }
  purcell.$e$$ion$[session_name].registerBeam = function(data) {
    //console.log("REG BEAM", data);
    purcell.$e$$ion$[session_name].GLOBAL_BEAM = data.beam[0]['id'];
  }
  purcell.$e$$ion$[session_name].beamOff = function() {
    purcell.$e$$ion$[session_name].GLOBAL_BEAM_FLAG = false;
    purcell.$e$$ion$[session_name].GLOBAL_BEAM = null;
    purcell.$e$$ion$[session_name].updateCurrentBeam();
  }
  purcell.$e$$ion$[session_name].s_ = function(v) {
    return 4.0 * v;
  }
  purcell.$e$$ion$[session_name].st_x = function(v) {
    return purcell.$e$$ion$[session_name].s_(v) + purcell.$e$$ion$[session_name].GLOBAL_X_SHIFT;
  }
  purcell.$e$$ion$[session_name].st_y = function(v) {
    return purcell.$e$$ion$[session_name].s_(v) + 30;
  }
  purcell.$e$$ion$[session_name].increment_and_execute = function(subsequent, out) {
    //console.log('IEX', out);
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
    //console.log("AAHHHHHH", out);
    out = {client:purcell.$e$$ion$[session_name].MY_NAME, sql:out, 'return': 'just_me', subsequent: subsequent};
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }

  purcell.$e$$ion$[session_name].add_dynamic = function(dynamic_id, note_id, out) {
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
      val : "'"+purcell.$e$$ion$[session_name].GLOBAL_DYNAMIC+"'"})
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

  purcell.$e$$ion$[session_name].addNote_2 = function(data) {
    //console.log("data going to AN_2", data);
    out = [];
    var prev = data['prev'][0]['id'];
    var prev_prev = data['prev_prev'][0]['id'];
    var next = data['next'][0]['id'];
    out.push({
      expected : [],
      sql : purcell.build_simple_insert('name',{id:next, val :
      purcell.$e$$ion$[session_name].GLOBAL_NOTE == null ? "'rest'" : "'note'"})
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
      val : purcell.$e$$ion$[session_name].GLOBAL_DURATION})
    });
    out.push({
      expected : [],
      sql : purcell.build_simple_insert('staff_symbol',{id:next,
      val : 1})
    });
    if (purcell.$e$$ion$[session_name].GLOBAL_NOTE != null) {
      if (purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL != null) {
        out.push({
          expected : [],
          sql : purcell.build_simple_insert('accidental',{id:next,
          val : purcell.$e$$ion$[session_name].GLOBAL_ACCIDENTAL})
        });
      }
      out.push({
        expected : [],
        sql : purcell.build_simple_insert('pitch',{id:next,
        val : purcell.$e$$ion$[session_name].GLOBAL_NOTE})
      });
      out.push({
        expected : [],
        sql : purcell.build_simple_insert('octave',{id:next,
        val : purcell.$e$$ion$[session_name].GLOBAL_OCTAVE})
      });
      // testing for beams!
      if (purcell.$e$$ion$[session_name].GLOBAL_BEAM_FLAG) {
        out.push({
          expected : [],
          sql : purcell.build_simple_insert('beam',{id:next,
          // ugh, for now just 5000...
          val : purcell.$e$$ion$[session_name].GLOBAL_BEAM})
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
    //console.log("checking data for dynamic id", data);
    if (data.dynamic_id != null) {
      out = purcell.$e$$ion$[session_name].add_dynamic(data.dynamic_id[0]['id'], next, out);
    }
    purcell.append_standard_graphical_queries(out);
    out = {client:purcell.$e$$ion$[session_name].MY_NAME, sql:out, 'return': "everyone", subsequent: "purcell.$e$$ion$."+session_name+".draw"};
    ///////////////////////////
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }
  purcell.$e$$ion$[session_name].addBarLine = function(data) {
    //console.log("data going to addBarLine", data);
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
    out = {client:purcell.$e$$ion$[session_name].MY_NAME, sql:out, 'return': "everyone", subsequent: "purcell.$e$$ion$."+session_name+".draw"};
    ///////////////////////////
    purcell.$e$$ion$[session_name].$0cket.send(JSON.stringify(out));
  }
  purcell.$e$$ion$[session_name].makeCurrentSelectedObjectColor = function(c) {
    var elt = null;
    for (var surface_index = 0; surface_index < purcell.$e$$ion$[session_name].$urfaces.length; surface_index++) {
      elt = purcell.$e$$ion$[session_name].$urfaces[surface_index].select('#'+purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT);
      if (elt) {
        break;
      }
    }
    if (elt.attr('fill') != 'none') {
      elt.attr({fill : c})
    }
    if (elt.attr('stroke') != 'none') {
      //console.log(elt.attr('stroke'))
      elt.attr({stroke : c})
    }  
  }
  purcell.$e$$ion$[session_name].registerAsClicked = function(id) {
    //console.log("SOMETHING",elt, $(elt).attr('id'));
    if (purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT) {
      purcell.$e$$ion$[session_name].makeCurrentSelectedObjectColor('black');
    }
    purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT = id;
    purcell.$e$$ion$[session_name].makeCurrentSelectedObjectColor('red');
  }
  purcell.$e$$ion$[session_name].draw = function(data) {
    //console.log("data going to draw", data)
    purcell.$e$$ion$[session_name].MAX_X = data.max_x[0]['x'];
    for (var surface_index=0; surface_index < purcell.$e$$ion$[session_name].$urfaces.length; surface_index++) {
      var current_surface = purcell.$e$$ion$[session_name].$urfaces[surface_index];
      current_surface.clear();
      _gr0up = current_surface.group().attr({transform:'scale(2,2);'});
      if (data.line_stencil != null) {
        for (var i = 0; i < data.line_stencil.length; i++) {
          var line = data.line_stencil[i];
          //console.log("making line", line);
          var x = parseFloat(!purcell.is_null(line['x_position']) ? line['x_position'] : 0.0);
          var y = parseFloat(!purcell.is_null(line['y_position']) ? line['y_position'] : 0.0);
          var name = line['name'] ? line['name'] : '';
          var s_line = _gr0up.line(purcell.$e$$ion$[session_name].st_x(parseFloat(line['x0']) + x),
                                   purcell.$e$$ion$[session_name].st_y(parseFloat(line['y0']) + y),
                                   purcell.$e$$ion$[session_name].st_x(parseFloat(line['x1']) + x),
                                   purcell.$e$$ion$[session_name].st_y(parseFloat(line['y1']) + y)).attr({
             stroke : 'black',
             strokeWidth : purcell.$e$$ion$[session_name].s_(parseFloat(line['thickness'])),
             'id' : name+'_line_'+line['id']+'_'+line['sub_id'],
          });
          //s_line.click(function() { return function() { purcell.$e$$ion$[session_name].registerAsClicked(s_line) } });
        }
      }
      if (data.glyph_stencil != null) {
        for (var i = 0; i < data.glyph_stencil.length; i++) {
          var closure = function () {
            var glyph = data.glyph_stencil[i];
            //console.log("making glyph", glyph);
            var x = parseFloat(glyph['x']) + parseFloat(!purcell.is_null(glyph['x_position']) ? glyph['x_position'] : 0.0);
            var y = parseFloat(glyph['y']) + parseFloat(!purcell.is_null(glyph['y_position']) ? glyph['y_position'] : 0.0);
            uc = String.fromCharCode(parseInt(glyph['unicode'].substr(2), 16));
            var name = glyph['name'] ? glyph['name'] : '';
            //console.log(context.font, glyph[3], uc);
            var s_glyph = _gr0up.text(purcell.$e$$ion$[session_name].st_x(x), purcell.$e$$ion$[session_name].st_y(y), uc).attr({
              "font-family" : glyph['font_name'],
              "font-size" : glyph['font_size'],
               'id' : name+'_glyph_'+glyph['id']+'_'+glyph['sub_id'],
            });
            //s_glyph.click(function() { purcell.$e$$ion$[session_name].registerAsClicked(s_glyph.attr('id'))  } );
          };
          closure();
        }
      }
      var polygon_holder = {};
      if (data.polygon_stencil != null) {
        for (var i = 0; i < data.polygon_stencil.length; i++) {
          var polygon = data.polygon_stencil[i];
          //console.log("PG",polygon);
          if (!polygon_holder[polygon['id']]) {
            polygon_holder[polygon['id']] = {};
          }
          if (!polygon_holder[polygon['id']][polygon['sub_id']]) {
            //console.log("making sub", polygon['sub_id']);
            polygon_holder[polygon['id']][polygon['sub_id']] = [];
          }
          polygon_holder[polygon['id']][polygon['sub_id']].push(polygon);
        }
        //console.log("PH", polygon_holder);
        for (key in polygon_holder) {
          for (sub_key in polygon_holder[key]) {
            // first, we sort in point order
            //console.log("before sorting", polygon_holder[key][sub_key])
            polygon_holder[key][sub_key].sort(function(a,b)
              {
                return parseInt(a['point']) - parseInt(b['point']);
              });
            //console.log("after sorting", polygon_holder[key][sub_key])
            // then, iterate
            var path = "";
            for (var i = 0; i < polygon_holder[key][sub_key].length; i++) {
              path = path + ((i == 0 ? 'M' : 'L') + " " + purcell.$e$$ion$[session_name].st_x(parseFloat(polygon_holder[key][sub_key][i].x)) + " " + purcell.$e$$ion$[session_name].st_y(parseFloat(polygon_holder[key][sub_key][i].y))+ " ");
            }
            var name = polygon_holder[key][sub_key][0]['name'] ? polygon_holder[key][sub_key][0]['name'] : '';
            var thick = parseFloat(polygon_holder[key][sub_key][0].thickness ? polygon_holder[key][sub_key][0].thickness : 0.0); 
            var s_polygon = _gr0up.path(path).attr({
              fill : polygon_holder[key][sub_key][0].fill == 1 ? true : false,
              stroke : 'black',
              strokeWidth : parseFloat(polygon_holder[key][sub_key][0].stroke) * thick,
             'id' : name+'_polygon_'+polygon_holder[key][sub_key][0]['id']+'_'+polygon_holder[key][sub_key][0]['sub_id'],
            });
            //s_polygon.click(function() { return function() { purcell.$e$$ion$[session_name].registerAsClicked(s_polygon) } });
          }
          
        }
      }
      //console.log("CSO", purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT);
        if (purcell.$e$$ion$[session_name].CURRENT_SELECTED_OBJECT) {
          purcell.$e$$ion$[session_name].makeCurrentSelectedObjectColor('red');
      }
    }
    $('#'+session_name+'_spinny').spin(false); // Stops and removes the spinner.
  }

  purcell.$e$$ion$[session_name].updateCurrentPitch();
  purcell.$e$$ion$[session_name].updateCurrentRhythm();
  purcell.$e$$ion$[session_name].updateCurrentAccidental();
  purcell.$e$$ion$[session_name].updateCurrentOctave();
  purcell.$e$$ion$[session_name].updateCurrentBeam();
  purcell.$e$$ion$[session_name].updateCurrentDynamic();
}
