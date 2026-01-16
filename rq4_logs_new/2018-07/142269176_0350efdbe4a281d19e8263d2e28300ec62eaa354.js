var canvas = document.getElementById("test");
var ctx = canvas.getContext("2d");
var lastend = 0;
var segmentNo = 50; // If you add more data values make sure you add more colors
var myColor = ['red', 'green', 'blue', 'yellow', 'white']; // Colors of each slice

for (var i = 0; i < segmentNo; i++) {
  	ctx.fillStyle = 'rgb(' + Math.floor(255/segmentNo*i) + ',' + Math.floor(255/segmentNo*(segmentNo-i)) + ',0)';
  	ctx.beginPath();
  	ctx.moveTo(canvas.width / 2, canvas.height / 2);
  	// Arc Parameters: x, y, radius, startingAngle (radians), endingAngle (radians), antiClockwise (boolean)
  	var x = canvas.width / 2;
  	var y = canvas.height / 2
  	var segmentAngle = Math.PI * 2 / segmentNo
  	ctx.arc(x, y, y, lastend, lastend + segmentAngle, false);
  	ctx.lineTo(canvas.width / 2, canvas.height / 2);
  	ctx.lineWidth = 5;
  	ctx.stroke();
  	ctx.fill();
  	lastend += Math.PI * 2 / segmentNo;
}