//console.log(" working")

//new image
var snorlax = new Image();
snorlax.src = "Snorlax.png"

//putting picture on canvas

snorlax.onload = function(){
	ctx4.drawImage(snorlax, 400, 100, 400, 400);
}

var Panda = new Image();
Panda.src = "Panda.png"

Panda.onload = function(){
	ctx4.drawImage(Panda, 100, 150, 400, 400);

}






var c = document.getElementById("MyCanvas");
var ctx = c.getContext('2d');
ctx.beginPath();
ctx.strokestyle = "blue";
ctx.moveTo(75,0);
ctx.lineTo(150,100);
ctx.lineTo(75,200);  
ctx.lineTo(0,100);
ctx.closePath();
ctx.stroke();
ctx.fillStyle = "blue";
ctx.fill();

var c2 = document.getElementById("MyCanvas2");
var ctx2 = c2.getContext('2d');
ctx2.beginPath();
ctx2.strokestyle = "red";
ctx2.moveTo(0,0);
ctx2.lineTo(300,300);
ctx2.lineTo(300,0);
ctx2.lineTo(0,300);
ctx2.stroke();
ctx2.fillStyle = "red";
ctx2.fill();

var c3 = document.getElementById('MyCanvas3');
var ctx3 =c3.getContext('2d');
ctx3.beginPath();
ctx3.arc(100,100,80,0,6.28);
ctx3.closePath();
ctx3.stroke();
ctx3.fillStyle = "pink";
ctx3.fill();

var c4 = document.getElementById("MyCanvas4")
var ctx4 = c4.getContext('2d');
//sky
ctx4.fillStyle = "cyan";
ctx4.fillRect(0,0,800,500);
//ground
ctx4.fillStyle = "green";
ctx4.fillRect(0,350,800,150);
//drawing sun
ctx4.moveTo(200,500);
ctx4.beginPath();
ctx4.arc(100,100,80,0,6.28);
ctx4.closePath();
ctx4.stroke();
ctx4.fillStyle = "yellow";
ctx4.fill();
//road
ctx4.beginPath();
ctx4.strokestyle = "grey";
ctx4.moveTo(300,350);
ctx4.lineTo(250,350);
ctx4.lineTo(220,500);
ctx4.lineTo(330,500);
ctx4.closePath();
ctx4.stroke();
ctx4.fillStyle = "gainsboro";
ctx4.fill();

ctx4.beginPath();
ctx4.strokestyle = "grey";
ctx4.moveTo(
	0,350);
ctx4.lineTo(250,350);
ctx4.lineTo(220,500);
ctx4.lineTo(330,500);
ctx4.closePath();
ctx4.stroke();
ctx4.fillStyle = "grey";
ctx4.fill();





ctx4.font = "italic 30pt Calibri";
ctx4.fillStyle= "tamato";
ctx4.fillText("welcome to my Scene!", 300, 50);