var LoP;
var LoT;
var ScreenWidth;
var ScreenHeight;
var Mohka;
function BoRayMaa(){
	ScreenWidth = screen.width;
	ScreenHeight = screen.height;
	LoP = window.innerWidth;
	LoT = window.innerHeight;
	window.resizeTo(ScreenWidth,ScreenHeight);
	Mohka = new Corsan("Mohka", 0,0,LoP,LoT,"rgb(255,255,255,255)");
	Mohka.Corsana.beginPath();
	Mohka.Corsana.moveTo(330,330);
	Mohka.Corsana.lineTo(520,130);
	Mohka.Corsana.stroke();
	document.getElementById("MohkaCorsan").addEventListener("touchstart", function(e){MeloTouchDown(e)});
	document.getElementById("MohkaCorsan").addEventListener("touchend", function(e){MeloTouchUp(e)});
	document.getElementById("MohkaCorsan").addEventListener("touchmove", function(e){MeloTouchMove(e)});
	document.getElementById("MohkaCorsan").addEventListener("mousedown", function(e){MeloMouseDown(e)});
	document.getElementById("MohkaCorsan").addEventListener("mouseup", function(e){MeloMouseUp(e)});
	document.getElementById("MohkaCorsan").addEventListener("mousemove", function(e){MeloMouseMove(e)});
	document.addEventListener("keypress", function(e) {
	    BeteverePlay(e.keyCode);
	});
}
function BeteverePlay(keycode){
	if (keycode == 96){
		var leMu = new Audio('dance.wav').play();
		
	}
}


function Corsan (Bo,mx,my,mw,mh, rgba) {
	         this.Corsan = document.createElement("canvas");
	         this.Corsan.id = Bo + "Corsan";
	         this.Corsan.width = mw;
	         this.Corsan.height = mh;
	         this.Corsan.style.position = "absolute";
	         this.Corsan.style.top = my;
	         this.Corsan.style.left = mx;
	         this.Corsan.style.width = mw;
	         this.Corsan.style.height = mh;
	         document.body.appendChild(this.Corsan);
		this.Bo = Bo;
		this.mx = mw;
		this.my = my;
		this.mw = mw;
		this.mh = mh;                                     //Brush
		this.Corsana = this.Corsan.getContext("2d");
		this.Corsana.rotation = 0;                   //BrushRotation
		                    this.RotateCorsana = function (rota){
		                    	this.Corsana.rotation = this.Corsana.rotation + rota;
		                    }
		 this.Corsana.fillStyle = "rgb(244,188,175)";
		 this.Corsana.strokeStyle = "rgb(84,11,74)";
		 this.Corsana.font = "27px Lucida Sans Unicode";
		 //this.Corsana.measureText("txt").width
		 //this.Corsana.strokeText("text",mx,my);
		//this.Corsana.fillText("text",mx,my);
		 this.Corsana.globalAlpha = 1; //                0 to 1
		 this.Corsana.lineCap = "round"; //          butt,round,square
		 this.Corsana.lineWidth = 2;
		 this.Corsana.lineJoin = "round";            //    bevel,round,miter
		 this.Corsana.miterLimit = 0;
		 this.Corsana.shadowBlur = 0;
		 this.Corsana.shadowColor = "Black";
		 this.Corsana,shadowOffsetX = 0;
		 this.Corsana.shadowOffsetY = 0;
		 this.Corsana.globalCompositeOperation = "source-over"; //could be destination-over for placing below previous

//		 this.Corsana.getImageData gets rectangel of (mx, my, mw, mh)
//		 this.Corsana.putImageData sets rectangel of (imgdata, mx, my)      img data obj has .width and .height
		 //this.Corsana.drawImage()   draws an image canvas or video      document.getElementById("imageID").onload.drawImage(img from get ele, mx, my);
		 
		 //Shapes
		 //this.Corsana.rect(mx,my,mw,mh);
		//this.Corsana.fillRect(mx,my,mw,mh);
		//this.Corsana.strokeRect(mx,my,mw,mh);
		//this.Corsana.clearRect(mx,my,mw,mh);
		 
		 //brushDraw
		// this.Corsana.beginPath();  this.Corsana.closePath(); start path and return to startpath.
		// this.Corsana.moveTo(mx,my);  moves without line
        // this.Corsana.lineTo(mx,my); moves with drawing
		 // this.Corsana.arc(centerX,CenterY,radius,0(startangel),Math.PI*2(circle,, endangel)
		 //this.Corsana.arcTo(mx1,my1,mx2,my2,radius) arc between two points
		 //this.Corsana.quadraticCurveTo(firstpulledtoX,firstpulledtoY,pointx,pointy) movetoloc first
		 //this.Corsana.bezierCurveTo(firstpulledtoX,firstpulledtoY,secondpulledtoX,secondpulledtoY,endpointx,endpointy)    from start position, "so 'moveto' first"
		 
		 //this.Corsana.fill();  for closed path
		 //this.Corsana.stroke();
		 //this.Corsana.isPointInPath(mx,my);
		 
		 //mods
		 //this.Corsana.rotate(degrees*Math.PI/180);
		 //this.Corsana.translate(x,y);
		 //this.Corsana.scale(mxX,myX);    multiple, with 2 being twice as large.  another 2 is twice that (stacking)

		 //saving
		 this.SaveToPng = function(){
			 window.location.href=canvas.toDataURL("image/png").replace("image/png", "image/octet-stream");
		 }
		 //this.Corsana.save();
		 //this.Corsana.restore();
		 
		 //gradients
		// var gradient = Corsana.createRadialGradient(mx1,my1,r1,mx2,my2,r2);
	     //var gradient = Corsana.createLinearGradient(mx1, my1, mx2, my2);
	     //gradient.addColorStop("0", "magenta");
	     //gradient.addColorStop("0.5", "blue");
	     //gradient.addColorStop("1.0", "red");  to 1
		 this.Corsana.linearGradient = this.Corsana.createLinearGradient(0,0,this.mw,this.mh);
		 this.Corsana.radialGradient = this.Corsana.createRadialGradient(0,0,0,this.mw,this.mh,Math.round(this.mh/2));
		 this.Corsana.radialGradient.addColorStop(0,"rgb(2,29,4)");
		 this.Corsana.radialGradient.addColorStop(1,"rgb(40,32,94)");
		 this.Corsana.linearGradient.addColorStop(0,"rgb(2,29,4)");
		 this.Corsana.linearGradient.addColorStop(1,"rgb(40,32,94)");
	     
	     
	     //MaaJi
		 
		 this.MaaAllodialTable = function(Titles){
			 
		    }
	     
	     //Custom Tech

		 //circuits 
		 this.TempWidth = 0;
		 this.evalArray = ["this.MaaAllodialTable();"]
		 this.Soam = setInterval(function(){
			 this.Corsana.clearRect(0,0,this.Corsan.width,this.Corsan.height);
			 
		 },100);
		 
}




function AllodialColumn(ara){
	this.datas = ara;
}













//TouchRouting
function MeloTouchDown(touch){
	var touch0 = touch.touches[0];
	var x = touch0.clientX;
	var y = touch0.clientY;
	MeloTouch(x,y);
}
function MeloTouchUp(touch){;
	var x = touch.clientX;
	var y = touch.clientY;
	MeloTouchRelease(x,y);
}
function MeloTouchMove(touch){
	var touch0 = touch.touches[0];
	var x = touch0.clientX;
	var y = touch0.clientY;
	//
}
function MeloMouseDown(touch){
	var x = touch.clientX;
	var y = touch.clientY;
	MeloTouch(x,y);
}
function MeloMouseUp(touch){
	var x = touch.clientX;
	var y = touch.clientY;
	MeloTouchRelease(x,y);
}
function MeloMouseMove(touch){
	var x = touch.clientX;
	var y = touch.clientY;
//
}
/////////////////////////////////////////////////////////////////////////////////////












function MeloTouch(x,y){
}

function MeloTouchRelease(x,y){
	
}