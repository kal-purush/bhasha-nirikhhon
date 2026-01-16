var bMouseClicked = false;
var intMouseX = 480;
var intMouseY = 300;
var strMouseStatus = "준비중";


var imgBackground = new Image();
imgBackground.src = "background.png";



var imgPlayer = new Image();
imgPlayer.src = "size_spongebob.png";

imgPlayer.addEventListener("load", drawScreen, false);

function drawScreen(){
	var theCanvas = document.getElementById("GameCanvas");
	var Context = theCanvas.getContext("2d");
	Context.drawImage(imgBackground,0,0,1000,700);
	Context.drawImage(imgPlayer,350,250,80,100);
}

window.addEventListener("load",drawScreen,false);
window.addEventListener("keydown",onkeydown,false);
window.addEventListener("keyup",onkeyup,false);
window.addEventListener("mousemove",onMouseMove,false);
window.addEventListener("mousedown",onMouseDown,false);
window.addEventListener("mouseup",onMouseUp,false);

var imgPlayer = new Image();
imgPlayer.src = "size_spongebob.png";
imgPlayer.addEventListener("load",drawScreen,false);

var strKeyEventType = "None";
var strKeyEventValue = "None";

function drawScreen(){
	var theCanvas = document.getElementById("GameCanvas");
	var Context = theCanvas.getContext("2d");
	Context.drawImage(imgBackground,0,0,1000,700);
	Context.drawImage(imgPlayer,350,250,50,55);
	/*Context.Style = "#00f";
	Context.font = '24px nanumgothic';
	Context.textBaseline = "top";
	Context.fillText("입력된 키는 :" + strKeyEventValue,5,5);
	Context.fillText("키 입력 상태는 : " + strKeyEventType,5,30);*/
	/*Context.fillStyle = "#ff0";
	Context.fillRect(0,0,1074,768);*/
	Context.drawImage(imgBackground,0,0,1000,700);
	Context.drawImage(imgPlayer, intMouseX, intMouseY);
	Context.fillStyle = "#000000";
	Context.font = '24px Arial';
	Context.textBaseline = "top";
	Context.fillText("발생한 마우스 이벤트는 :" + strMouseStatus,5,5);
	Context.fillText("마우스 좌표는 : " + "X = " +intMouseX + " Y = "+ intMouseY,5,30);


}

function onMouseDown(e){
	strMouseStatus = "클릭!";
	var theCanvas = document.getElementById("GameCanvas");
	bMouseClicked = true;
	intMouseX = e.clientX - theCanvas.offsetLeft-42;
	intMouseY = e.clientY - theCanvas.offsetTop-50;
	drawScreen();
}


function onMouseMove(e){
	strMouseStatus = "Moving now";
	if (bMouseClicked) {
		var theCanvas = document.getElementById("GameCanvas");
		bMouseClicked = true;
		intMouseX = e.clientX - theCanvas.offsetLeft-42;
		intMouseY = e.clientY - theCanvas.offsetTop-50;
		drawScreen();
	}
}

function onMouseUp(e) {
	strMouseStatus = "클릭 끝!";
	bMouseClicked = false;
	intMouseX = 480;
	intMouseY = 300;
	drawScreen();
}

/*function onkeydown(e)
{
	strKeyEventType = e.type;
	if(e.keyCode)code = e.keyCode;
	strKeyEventValue = code;
	drawScreen();
}

function onkeyup(e)
{
	strKeyEventType = e.type;
	if(e.keyCode)code = e.keyCode;
	strKeyEventValue = String.fromCharCode(code);
	drawScreen();
}*/