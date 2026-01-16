var canvas = document.getElementById("canvas");
var context = canvas.getContext('2d');
var HEIGHT = WIDTH = 800;
canvas.height = HEIGHT;
canvas.width = WIDTH;

STATS = {frame_count:0}

Array.prototype.sumArray = function (arr) {
  var sum = [];
  if (arr != null && this.length == arr.length) {
    for (var i = 0; i < arr.length; i++) {
      sum.push(this[i] + arr[i]);
    }
  }

  return sum;
}

var TIMERS = {};

function functionName(fun) {
  var ret = fun.toString();
  ret = ret.substr('function '.length);
  ret = ret.substr(0, ret.indexOf('('));
  return ret;
}

function timeIt(f) {
  var name = functionName(f);
  TIMERS[name] = [];
  return function() {
    var start = new Date().valueOf()
    f();
    TIMERS[name].push(new Date().valueOf()-start);
  }
}

var CELLW,NROWS,NCOLS,GAME_STATE,RGB;
var FORCE_CHANGE = true;

var CMAP = {
  0: {
    0: { 0: '#000', 1: '#00F', },
    1: { 0: '#0F0', 1: '#0FF', },
  },
  1: {
    0: { 0: '#F00', 1: '#F0F', },
    1: { 0: '#FF0', 1: '#FFF', }
  }
}

var COLORS = [
 '#FFF',
 '#FF0',
 '#F0F',
 '#F00',
 '#0FF',
 '#0F0',
 '#00F',
 '#000',
]

var b_numbers = [3];
var s_numbers = [2,3];

var ui = (function(){
  var game_context = window.context;
  var canvas = document.createElement("canvas");
  var current_color = "#FFF";
  canvas.width = WIDTH;
  canvas.height = HEIGHT;
  canvas.id = "ui";
  var context = canvas.getContext("2d");
  document.getElementById("wrapper").appendChild(canvas);
  var _h,_c;
  function draw() {
    context.clearRect(0,0,WIDTH,HEIGHT);
    //draw grid
    for (var i=1;i<NROWS;i++) { rect({x:0,y:i*CELLW,h:1,w:WIDTH,color:"gray"},context) }
    for (var i=1;i<NCOLS;i++) { rect({x:i*CELLW,y:0,h:HEIGHT,w:1,color:"gray"},context) }

    // hover and click
    if (_h) {
      rect({x:_h[0]*CELLW,y:_h[1]*CELLW,w:CELLW,h:CELLW,color:"rgba(255,255,255,0.5)"},context);
    }
    if (_c) {
      color = current_color;
      rect({x:_c[0]*CELLW,y:_c[1]*CELLW,w:CELLW,h:CELLW,color:color},game_context);
      _c = undefined;
      game.getStateFromCanvas();
    }
  }
  canvas.addEventListener("mousemove",function(e) {_h = [~~(e.layerX/CELLW),~~(e.layerY/CELLW)];});
  canvas.addEventListener("mousedown",function(e) {_c = [~~(e.layerX/CELLW),~~(e.layerY/CELLW)];});
  canvas.addEventListener("mouseout",function(e) {_h=undefined;});
  var color_picker = document.createElement("select");
  color_picker.id = "color_picker";
  for (var i=0;i<COLORS.length;i++) {
    var option = document.createElement("option");
    option.value = COLORS[i];
    option.innerHTML = COLORS[i];
    var swatch = document.createElement("span");
    swatch.className = "swatch";
    swatch.style.backgroundColor = COLORS[i];
    option.appendChild(swatch)
    color_picker.appendChild(option);
  }
  document.getElementById("wrapper").appendChild(color_picker);
  function chooseColor() {
    current_color = document.getElementById("color_picker").value;
  }
  color_picker.addEventListener("change",chooseColor);

  var interval;
  function start() {
    clearInterval(interval);
    interval = setInterval(draw,50);
  }
  start();
  function stop() {
    context.clearRect(0,0,WIDTH,HEIGHT);
    clearInterval(interval);
  }
  return {
    'context': context,
    'start': start,
    'stop': stop
  }
})();

var game = (function() {
  var CHANGED;
  function EmptyRGB() { 
    var out = [];
    for (var row=0;row<NROWS*NCOLS;row++) {
      out.push([0,0,0]);
    }
    return out
  }
  function resetState () {
    RGB = new EmptyRGB();
    CHANGED = new Array(RGB.length);
  }

  var current_zoom = 1;
  var available_zooms = [5,10,20,40,80,160]
  //var available_zooms = [5,10,15,20,25,30,35,40,45,50]
  function zoomOut() {
    if (current_zoom == 0) { return }
    current_zoom--;
    init();
  }

  function zoomIn() {
    if (current_zoom == available_zooms.length-1) { return }
    current_zoom++;
    init();
  }

  function init() {
    CELLW = available_zooms[current_zoom];
    NROWS = Math.floor(HEIGHT/CELLW);
    NCOLS = Math.floor(WIDTH/CELLW);
    resetState();
    getStateFromCanvas();
    FORCE_CHANGE = true;
  }
  init();

  var getCellByIndex = (function() {
    //cache these two for later
    ROW_MAX = NROWS-1;
    COL_MAX = NCOLS-1;
    return function (row,col) {
      if (row == -1) { row = NROWS -1; }
      else if (row == NROWS) { row = 0; }
      if (col == -1) { col = NCOLS -1; }
      else if (col == NCOLS) { col = 0; }
      return RGB[row*NCOLS+col];
    }
  })()

  function getStateFromCanvas() {
    resetState();
    var _r;
    var data = context.getImageData(0,0, CELLW*NCOLS, CELLW*NROWS).data;
    var i = data.length/4;
    count = 0;
    while(i--) {
      count +=1;
      row = Math.floor(i/(NCOLS*CELLW*CELLW));
      col = Math.floor(i/CELLW)%(NCOLS);
      RGB[NCOLS*row+col][0] += data[i*4];
      RGB[NCOLS*row+col][1] += data[i*4+1];
      RGB[NCOLS*row+col][2] += data[i*4+2];
    }
    var norm = CELLW*CELLW*255;
    for (var i=0;i<NROWS*NCOLS;i++) {
      RGB[i][0] = Math.round(RGB[i][0]/norm);
      RGB[i][1] = Math.round(RGB[i][1]/norm);
      RGB[i][2] = Math.round(RGB[i][2]/norm);
    }
  }

  nextGameState = timeIt(nextGameState);
  function nextGameState() {
    var new_state = new EmptyRGB();
    var neighbors_alive,cell_number;
    for (var row=0;row<NROWS;row++) {
      for (var col=0;col<NCOLS;col++) {
        cell_number = row*NROWS+col;
        neighbors_alive = [0,0,0];
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row-1,col+1));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row-1,col));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row-1,col-1));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row,col+1));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row,col-1));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row+1,col+1));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row+1,col));
        neighbors_alive = neighbors_alive.sumArray(getCellByIndex(row+1,col-1));
        CHANGED[cell_number] = false;
        for (var i=0;i<3;i++) {
          if (RGB[cell_number][i] == 0) { //cell is dead
            new_state[cell_number][i] = 1*(b_numbers.indexOf(neighbors_alive[i]) > -1);
          } else { // cell i alive
            new_state[cell_number][i] = 1*(s_numbers.indexOf(neighbors_alive[i]) > -1);
          }
          if (new_state[cell_number][i] != RGB[cell_number][i]) { CHANGED[cell_number] = true; }
        }
        //console.log(" r"+row+" c"+col+" na"+neighbors_alive+" new"+new_state[cell_number]);
      }
    }
    RGB = new_state;
  }

  function tick() {
    var start = new Date().valueOf();
    STATS.frame_count++;
    var time_passed = (start - STATS.start_time)/1000;
    document.getElementById("fps").innerHTML = STATS.frame_count / time_passed;
    nextGameState();
    draw();
    var mspf = new Date().valueOf()-start;
    document.getElementById("mspf").innerHTML = mspf;
  }

  function draw() {
    for (var row=0;row<NROWS;row++) {
      for (var col=0;col<NCOLS;col++) {
        if (!(FORCE_CHANGE || CHANGED[NCOLS*row+col])) { continue }
        var _c = getCellByIndex(row,col);
        var color = CMAP[_c[0]][_c[1]][_c[2]];
        rect({x:col*CELLW,y:row*CELLW,w:CELLW,h:CELLW,color:color},context);
      }
    }
    FORCE_CHANGE = false;
  }
  return {
    'getStateFromCanvas': getStateFromCanvas,
    'draw': draw,
    'tick': tick,
    'getCellByIndex': getCellByIndex,
    'zoomIn': zoomIn,
    'zoomOut': zoomOut,
  }
})()

function rect(r,ctx) {
  ctx.fillStyle = r.color;
  ctx.beginPath();
  ctx.rect(r.x, r.y, r.w, r.h);
  ctx.fill();
  ctx.closePath();
}

(function() {
  game.draw();
  game.getStateFromCanvas();
})()

function update_numbers() {
  window.b_numbers = document.getElementById("b_numbers").value.split(',');
  for(var i=0; i<window.b_numbers.length; i++) { window.b_numbers[i] = +window.b_numbers[i]; } 
  window.s_numbers = document.getElementById("s_numbers").value.split(',');
  for(var i=0; i<window.s_numbers.length; i++) { window.s_numbers[i] = +window.s_numbers[i]; } 
  STATS.frame_count = 0;
  STATS.start_time = new Date().valueOf();
}
var interval;
function start() {
  stop();
  ui.stop();
  update_numbers();
  window.interval = setInterval(game.tick,500);
}
function startx2() {
  stop();
  ui.stop();
  update_numbers();
  window.interval = setInterval(game.tick,200);
}
function stop() {
  clearInterval(window.interval)
  ui.start();
}

auto = document.getElementById("auto");
auto.addEventListener("mouseover",start);
auto.addEventListener("mouseout",stop);
autox2 = document.getElementById("autox2");
autox2.addEventListener("mouseover",startx2);
autox2.addEventListener("mouseout",stop);