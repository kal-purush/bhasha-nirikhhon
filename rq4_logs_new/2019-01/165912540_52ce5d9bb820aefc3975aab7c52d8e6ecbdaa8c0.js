/*
Zeye Gu 101036562
Ziwen Wang 101071063


*/


const socket = io('http://' + window.document.location.host)

var stone1 = {
  id:1,
  x: 20,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,
  color: "red",
  col:[]
  
};
var stone2 = {
  id:2,
  x: 70,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,
  color: "red",
  col:[]
  
};

var stone3 = {
  id:3,
  x: 120,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,
  color: "red",
  col:[]
  
};
var stone4 = {
  id:4,
  x: 170,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,

  color: "blue",
  col:[]
  
};
var stone5 = {
  id:5,
  x: 220,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,
  color: "blue",
  col:[]
  
};
var stone6  = {
  id:6,
  x: 270,
  y: 565,
  xDirection: 0, //+1 for leftwards, -1 for rightwards
  yDirection: 0, //+1 for downwards, -1 for upwards
  radius: 20,
  color: "blue",
  col:[]
  
};


var backGound  = {
  x: 150,
  y: 150,
  radius: 100,
  
};

var stones = [stone1, stone2, stone3, stone4, stone5, stone6]
var player = ""
var name = "User"
var turn = false
var moved = false

let movingString = {
  word: "Hello",
  x: 100,
  y: 100,
  xDirection: 1, //+1 for leftwards, -1 for rightwards
  yDirection: 1, //+1 for downwards, -1 for upwards
  stringWidth: 50, //will be updated when drawn
  stringHeight: 24
} //assumed height based on drawing point size

//intended for keyboard control


let timer //used to control the free moving word
let pollingTimer //timer to poll server for location updates

let wordBeingMoved //word being dragged by mouse
let wordTargetRect = {
  x: 0,
  y: 0,
  width: 0,
  height: 0
} //bounding box around word being targeted

let deltaX, deltaY //location where mouse is pressed
//let canvas = document.getElementById("canvas1") //our drawing canvas
let canvas = document.getElementById("canvas2") //our drawing canvas
const fontPointSize = 18 //point size for word text
const wordHeight = 20 //estimated height of a string in the editor
const editorFont = "Arial" //font for your editor

var tri = {sx:0,sy:0,ex:0,ey:0}

function drawCanvas() {
  let context = canvas.getContext("2d");

  context.fillStyle = "white";
  context.fillRect(0, 0, canvas.width, canvas.height); //erase canvas

  context.font = "" + fontPointSize + "pt " + editorFont;
  context.fillStyle = "cornflowerblue";
  context.strokeStyle = "blue";

  
  movingString.stringWidth = context.measureText(movingString.word).width;
  context.fillText(movingString.word, movingString.x, movingString.y);



  
  
  //draw background
  context.beginPath()
  context.arc(backGound.x, backGound.y, backGound.radius, 0, 2*Math.PI);
  context.lineWidth =25;
  context.strokeStyle ='blue'
  context.closePath()
  context.stroke()
  
  context.beginPath()
  context.arc(backGound.x, backGound.y, backGound.radius-2*25, 0, 2*Math.PI);
  context.lineWidth =25;
  context.strokeStyle ='red'
  context.closePath()
  context.stroke()
  
  //draw circle
	
	stones.forEach(function(item, index, arr){
		context.beginPath()
		context.fillStyle = item.color;
		context.arc(item.x, item.y, item.radius, 0, 2*Math.PI);
		context.closePath()
		context.fill()
	})
  
  context.beginPath()
  context.moveTo(tri.sx, tri.sy)
  let xd = tri.sx - tri.ex
  let yd = tri.sy - tri.ey
  let a = Math.atan(xd/yd)+Math.PI/2
  let l = Math.sqrt(Math.pow(xd,2) + Math.pow(yd,2))/8
  context.lineTo(l*Math.sin(a)+tri.ex, l*Math.cos(a)+tri.ey)
  context.lineTo(l*Math.sin(a+Math.PI)+tri.ex, l*Math.cos(a+Math.PI)+tri.ey)
  context.lineWidth =1
  context.strokeStyle = "black"
  context.closePath()
  context.stroke();

  
 
  drawLarge()

}
function drawLarge() {
  let canvas = document.getElementById("canvas1")
  let context = canvas.getContext("2d");

  context.fillStyle = "white";
  context.fillRect(0, 0, canvas.width, canvas.height); //erase canvas
  
  
  //draw background
  context.beginPath()
  context.arc(2*backGound.x, 2*backGound.y, 2*backGound.radius, 0, 2*Math.PI);
  context.lineWidth =50;
  context.strokeStyle ='blue'
  context.closePath()
  context.stroke()
  
  
  context.beginPath()
  context.arc(2*backGound.x, 2*backGound.y, 2*backGound.radius-2*50, 0, 2*Math.PI);
  context.lineWidth =50;
  context.strokeStyle ='red'
  context.closePath()
  context.stroke()
	//draw circle
  
	stones.forEach(function(item, index, arr){
		context.beginPath()
		context.fillStyle = item.color;
		context.arc(item.x * 2, item.y * 2, item.radius * 2, 0, 2*Math.PI);

		context.fill()
	})
	
	
  
}



function handleMouseDown(e) {
  //get mouse location relative to canvas top left
  let rect = canvas.getBoundingClientRect()
  //var canvasX = e.clientX - rect.left
  //var canvasY = e.clientY - rect.top
  let canvasX = e.pageX - rect.left //use  event object pageX and pageY
  let canvasY = e.pageY - rect.top
  console.log("mouse down:" + canvasX + ", " + canvasY)
  tri = {sx:canvasX,sy:canvasY,ex:canvasX,ey:canvasY}


  //console.log(wordBeingMoved.word)
  //if (wordBeingMoved != null) {
  /*if (true) {*/
    //deltaX = wordBeingMoved.x - canvasX
    //deltaY = wordBeingMoved.y - canvasY
    //attache mouse move and mouse up handlers
    $(document).mousemove(handleMouseMove)
    $(document).mouseup(handleMouseUp)
  /*}*/

  // Stop propagation of the event and stop any default
  //  browser action
  e.stopPropagation()
  e.preventDefault()

  drawCanvas()
}

function handleMouseMove(e) {
  console.log("mouse move");

  //get mouse location relative to canvas top left
  let rect = canvas.getBoundingClientRect()
  let canvasX = e.pageX - rect.left
  let canvasY = e.pageY - rect.top

  //wordBeingMoved.x = canvasX + deltaX
  //wordBeingMoved.y = canvasY + deltaY
  tri = {sx:tri.sx,sy:tri.sy,ex:canvasX,ey:canvasY}
  e.stopPropagation()
  
  drawCanvas()
}

function handleMouseUp(e) {
	console.log("mouse up")
	e.stopPropagation()
	
	//remove mouse move and mouse up handlers but leave mouse down handler
	$(document).off("mousemove", handleMouseMove); //remove mouse move handler
	$(document).off("mouseup", handleMouseUp); //remove mouse up handler
	console.log(tri)
	
	stones.forEach(function(item, index, arr){
		if(Math.sqrt(Math.pow(item.x-tri.sx,2) + Math.pow(item.y-tri.sy,2))<20 && item.color === player){
			item.xDirection = (tri.sx - tri.ex)/10
			item.yDirection = (tri.sy - tri.ey)/10
			console.log(item)
		}
	})
	tri = {sx:0,sy:0,ex:0,ey:0}
  	drawCanvas() //redraw the canvas
	setTimeout(function(){moved = true}, 100)
}


function sign(num){
	if (num > 0) return 1
	if (num < 0) return -1
	return 0
}
function handleTimer() {
	if(player && turn){
		let noMove = true;
		stones.forEach(function(item, index, arr){
			item.x += item.xDirection
			item.y += item.yDirection
			item.xDirection = item.xDirection * 0.97
			item.yDirection = item.yDirection * 0.97
			if(Math.abs(item.xDirection) > 0.01 && Math.abs(item.yDirection) > 0.01)
				noMove = false;
		})
		stones.forEach(function(item, index, arr){
			//wall
			if (item.x + item.radius > canvas.width) item.xDirection = sign(canvas.width - (item.x + item.radius)) * Math.abs(item.xDirection)
			if (item.x - item.radius < 0) item.xDirection = sign(0 - (item.x - item.radius)) * Math.abs(item.xDirection)
			if (item.y + item.radius > canvas.height) item.yDirection = sign(canvas.height - (item.y + item.radius)) * Math.abs(item.yDirection)
			if (item.y - item.radius < 0) item.yDirection = sign(0 - (item.y - item.radius)) * Math.abs(item.yDirection)
			//other stone
			stones.forEach(function(item2, index, arr){
				if(Math.sqrt(Math.pow(item2.x-item.x,2) + Math.pow(item.y-item2.y,2))<40 && item != item2){
					if(!item2.col.includes(item.id)){
						item2.col.push(item.id)
						item.col.push(item2.id)
						let ax = item.xDirection
						let ay = item.yDirection
						let bx = item2.xDirection
						let by = item2.yDirection
						item2.xDirection = ax*0.75 + bx*0.25
						item2.yDirection = ay*0.75 + by*0.25
						item.xDirection = ax*0.25 + bx*0.75
						item.yDirection = ay*0.25 + by*0.75
					}
				}else{
					var index = item2.col.indexOf(item.id);
					if (index > -1) {
						item2.col.splice(index, 1);
					}
				}
			})/*
			vy = sqrt(item.xDirection^2 + item.yDirection^2)
			a = (actan(item.xDirection/item.yDirection))-actan((item.x - item2.x)/(item.y - item2.y))
			b = (actan(item2.xDirection/item2.yDirection))-actan((item.x - item2.x)/(item.y - item2.y))
			Uy =.....
			item.xDirection Uy*cos(a)....*/
		})		
		
		
		//if(sendcount > 1){
		let dataObj = {stones:stones,player:player}
		let jsonString = JSON.stringify(dataObj)
		socket.emit('move', jsonString)
		if(noMove && moved){
			socket.emit('finturn', "")
			moved = false
		}
			//sendcount = 0
		//}
		//sendcount += 1
	}
	
  movingString.x = movingString.x + 5 * movingString.xDirection
  movingString.y = movingString.y + 5 * movingString.yDirection
  

  //keep moving word within bounds of canvas
  if (movingString.x + movingString.stringWidth > canvas.width) movingString.xDirection = -1
  if (movingString.x < 0) movingString.xDirection = 1
  if (movingString.y > canvas.height) movingString.yDirection = -1
  if (movingString.y - movingString.stringHeight < 0) movingString.yDirection = 1

  drawCanvas()
}

//KEY CODES
//should clean up these hard coded key codes
const RIGHT_ARROW = 39
const LEFT_ARROW = 37
const UP_ARROW = 38
const DOWN_ARROW = 40


socket.on('serverSays', function(message) {
	var locationData = JSON.parse(message)

})

socket.on('move', function(message) {
	//console.log(message)
	stones = JSON.parse(message)
	drawCanvas()
})

function handleKeyDown(e) {
  console.log("keydown code = " + e.which);
/*if (e.which == 82) {
	  player = "red"
	 $("#canvas2").mousedown(handleMouseDown)
  }
 if (e.which == 66) {
	  player = "blue"
	  $("#canvas2").mousedown(handleMouseDown)
  }
*/
  let dXY = 5; //amount to move in both X and Y direction

  //create a JSON string representation of the data object
  let jsonString = JSON.stringify(dataObj)

  //update the server with a new location of the moving box
  /*$.post("positionData", jsonString, function(data, status) {
    //do nothing
  })*/
  
  socket.emit('clientSays', jsonString)
}

function handleKeyUp(e) {
  console.log("key UP: " + e.which)

  //create a JSON string representation of the data object
  let jsonString = JSON.stringify(dataObj)
  socket.emit('clientSays', jsonString)
  /*$.post("positionData", jsonString, function(data, status) {
    console.log("data: " + data)
    console.log("typeof: " + typeof data)
    //do nothing;
  })*/
}



function handleRegisterRed(){
	name = document.getElementById("player").value// get name from textfield
	if(name ==="")name = "User1"
	let player = "red"
	let dataObj = {name:name,player:player}
	let jsonString = JSON.stringify(dataObj)
	socket.emit('reg', jsonString)
}
function handleRegisterBlue(){
	name = document.getElementById("player").value
	if(name ==="")name = "User2"
	let player = "blue"
	let dataObj = {name:name,player:player}
	let jsonString = JSON.stringify(dataObj)
	socket.emit('reg', jsonString)
}
function handleGiveUpButton(){
	let dataObj = {name:name,player:player}
	let jsonString = JSON.stringify(dataObj)
	socket.emit('dereg', jsonString)
}

socket.on('confirmreg', function(message) {
	var data = JSON.parse(message)
	if(data.color !== ""){
		player = data.color
		window.alert("now you are " + data.color);
	}else{
		window.alert("reg failed");
	}
	$("#canvas2").mousedown(handleMouseDown)
})

socket.on('startturn', function(message){
	$("#canvas2").mousedown(handleMouseDown)
	window.alert("you can start");
	turn = true
})
socket.on('endturn', function(message){
	$("#canvas2").off("mousedown", handleMouseDown)
	window.alert("end turn");
	turn = false
})

socket.on('playername', function(message){
	var data = JSON.parse(message)
	console.log(data)// print name
	if(data.red !==""||data.blue !== ""){
		if(data.red !=="")
			document.getElementById("player1Infor").innerHTML=`Red: ${data.red}`
		else{
			document.getElementById("player1Infor").innerHTML="Waiting for player"
		}
		
		if(data.blue !== "")
			document.getElementById("player2Infor").innerHTML= `Blue: ${data.blue}`
		else{
			document.getElementById("player2Infor").innerHTML="Waiting for player"
		}
	}
	else{
		document.getElementById("player1Infor").innerHTML="Waiting for player"
		document.getElementById("player2Infor").innerHTML="Waiting for player"
	}
	
})


$(document).ready(function() {
  //add mouse down listener to our canvas object
  //add keyboard handler to document
  ///$(document).keydown(handleKeyDown)
  ////$(document).keyup(handleKeyUp)
  timer = setInterval(handleTimer, 100) //tenth of second
  //pollingTimer = setInterval(pollingTimerHandler, 100) //quarter of a second
  //clearTimeout(timer) //to stop

  drawCanvas()
})