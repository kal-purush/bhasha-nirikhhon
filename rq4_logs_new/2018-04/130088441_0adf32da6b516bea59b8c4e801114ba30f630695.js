const canvas = document.getElementById("canvas");
const ctx = canvas.getContext("2d");
const valueDiv = document.getElementById("value");
const valueAngle = document.getElementById("showangle");

const width = canvas.width / 2;
const height = canvas.height;


var startValue = 1;
const startAngle = -Math.PI / 2;

ctx.fillStyle = "#808080";
ctx.fillRect(0, 0, canvas.width, canvas.height);

var lineWidth = 2;
var angle = 25*(Math.PI)/180
var len = 60;
var x = width;
var y = height;
var dir = startAngle;
var array = [];
var trunk = 100;

fractalBinaryTree0(startValue,len,5,0);
valueDiv.innerHTML = startValue;

document.getElementById("angle").oninput = function() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#808080";
  ctx.fillRect(0, 0, canvas.width, canvas.height)
  valueAngle.innerHTML = this.value;

  x = width;
  y = height;
  dir = startAngle;
  angle = this.value*Math.PI/180;

  fractalBinaryTree0(document.getElementById("input").value,len,5,0);
}

document.getElementById("input").oninput = function() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#808080";
  ctx.fillRect(0, 0, canvas.width, canvas.height)
  valueDiv.innerHTML = this.value;

  x = width;
  y = height;
  dir = startAngle;
  angle = document.getElementById("angle").value*Math.PI/180;
  startValue = this.value
	
  fractalBinaryTree0(this.value,len,5,0);
}

function fractalBinaryTree0(num,length,width, color) {

  if (num == startValue ){
      drawLeaf(trunk,width,color);
  }

  if (num > 0) {
    push();
    dir += -angle;
    drawLeaf(length,width,color);
    fractalBinaryTree0(num-1,length*0.9,width*0.8, color +1);
    pop();
    dir += angle;
    drawLeaf(length,width,color);
    fractalBinaryTree0(num-1,length*0.9,width*0.8, color +1);

  }

}

function push(){
  array.push(x);
  array.push(y);
  array.push(dir);
}

function pop(){
  dir = array.pop();
  y = array.pop();
  x = array.pop();
}

function drawLeaf(length,width,color) {
  var col = 10+20*color;
  col = col.toString(16);
  if(col.length == 1){
    col = "0" + col;
  }
  ctx.beginPath();
  ctx.lineWidth = width;
  ctx.strokeStyle = "#14"+ col + "00";
  ctx.moveTo(x, y);
  x+= Math.cos(dir) * length;
  y += Math.sin(dir) * length;

  ctx.lineTo(x, y);
  ctx.stroke(); // Draw it
}