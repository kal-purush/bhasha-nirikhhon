var CanvasChart = function(){
this.width;
this.height;
this.ctx;
this.ctx_alert;
this.ctx_alarm;
this.alertYpos = 0;
this.total;
this.max_y;
this.start_timestamp;
this.zero_timestamp;
this.timestamp;
this.context;
this.lastAlertTimestamp;
this.alarmContainer;
this.baseShift_x=100000;
this.firstPoint=true;
this.alarmParams;
this.originX=25;
this.originY=30;
this.m_f;
this.lastPoint={x:0,y:0};
this.alarmPoints=[];
this.canvas;
this.canvas_alert;
this.canvas_alarm;
this.canvas_x;
this.canvas_y;
this.canvas_x_caption;
this.canvas_y_caption;

}



CanvasChart.prototype.init =  function(context , div_name , height , start_timestamp , index , color , max_y , max_x, alarmParams,strokeBuffer ){
		this.context = context;
		this.max_x = max_x;
		this.max_y = max_y;
		this.m_f=1;
		var m_f=1;
		var div_e = document.getElementById(div_name);

		this.width = div_e.clientWidth;
		this.height = height;
	    this.canvas = document.createElement('canvas');
		 this.canvas.id     = "xyz";
         this.canvas.width  = this.width;
         this.canvas.height = this.height;
		 this.canvas.style.zIndex   = 3;
         this.canvas.style.position = "absolute";
		this.alarmContainer={};
		this.alarmParams=alarmParams;
		

		this.canvas_alert = document.createElement('canvas');
		 this.canvas_alert.id     = "xyz";
         this.canvas_alert.width  = this.width;
         this.canvas_alert.height = this.height;
		 this.canvas_alert.style.zIndex   = 5;
         this.canvas_alert.style.position = "absolute";
		
		this.canvas_alarm = document.createElement('canvas');
		 this.canvas_alarm.id     = "xyz";
         this.canvas_alarm.width  = this.width;
         this.canvas_alarm.height = this.height;
		 this.canvas_alarm.style.zIndex   = 4;
         this.canvas_alarm.style.position = "absolute";

 		
		
		 this.canvas_x = document.createElement('canvas');
		 this.canvas_x.width  = this.width;
		 this.canvas_x.height = this.height;
		 this.canvas_x.style.zIndex = 1
		 this.canvas_x.style.position = "absolute";
		this.ctx_x =  this.canvas_x.getContext("2d");
		this.ctx_x.font= "12px Georgia"

		 this.canvas_x_caption = document.createElement('canvas');
		 this.canvas_x_caption.width  = this.width;
		 this.canvas_x_caption.height = this.height;
		 this.canvas_x_caption.style.zIndex = 1
		 this.canvas_x_caption.style.position = "absolute";
		this.ctx_x_caption =  this.canvas_x_caption.getContext("2d");
		this.ctx_x_caption.font= "12px Georgia";
			
		
		

		
		 this.canvas_y = document.createElement('canvas');
		 this.canvas_y.width  = 60;
		 this.canvas_y.height = this.height;
		 this.canvas_y.style.zIndex = 1
		 this.canvas_y.style.position = "absolute";
		 this.ctx_y =  this.canvas_y.getContext("2d");
		this.ctx_y.rotate(90*Math.PI/180);
				this.ctx_y.font= "12px Georgia";
				
		 this.canvas_y_caption = document.createElement('canvas');
		 this.canvas_y_caption.width  = 80;
		 this.canvas_y_caption.height = this.height;
		 this.canvas_y_caption.style.zIndex = 1
		 this.canvas_y_caption.style.position = "absolute";
		this.ctx_y_caption =  this.canvas_y_caption.getContext("2d");
		this.ctx_y_caption.rotate(90*Math.PI/180);
				this.ctx_y_caption.font= "12px Georgia";

		
		
        div_e.appendChild(this.canvas);
		div_e.appendChild(this.canvas_alert);
		div_e.appendChild(this.canvas_alarm);
		div_e.appendChild(this.canvas_x);
		div_e.appendChild(this.canvas_y);
		div_e.appendChild(this.canvas_x_caption);
		div_e.appendChild(this.canvas_y_caption);
		
		this.ctx= this.canvas.getContext("2d");
		this.ctx_alert =  this.canvas_alert.getContext("2d");
		this.ctx_alarm =  this.canvas_alarm.getContext("2d");

		 this.canvas.addEventListener('mousemove', function(evt) {
        var mousePos = getMousePos(canvas, evt);
        var message = 'Mouse position: ' + mousePos.x + ',' + mousePos.y;
        console.log(message);
      	}, false);

		
		this.ctx_alarm.font="bold 12px Georgia";
		//this.ctx.font="bold 12px Georgia";
		

		if(this.alarmParams.order!=null){


	if(this.alarmParams.order=="ryg"){
	var x=25;
	

	this.ctx.fillStyle=getColorString("red");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.r_e*m_f),this.width,this.getYPos(this.alarmParams.r_s*m_f)-this.getYPos(this.alarmParams.r_e*m_f));
	this.ctx.fillStyle=getColorString("yellow");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.y_e*m_f),this.width,this.getYPos(this.alarmParams.y_s*m_f)-this.getYPos(this.alarmParams.y_e*m_f));
	this.ctx.fillStyle=getColorString("green");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.g_e*m_f),this.width,this.getYPos(this.alarmParams.g_s*m_f)-this.getYPos(this.alarmParams.g_e*m_f));



	}
	else if(this.alarmParams.order=="gyr"){
	var x=25;
	


	this.ctx.fillStyle=getColorString("red");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.r_e*m_f),this.width,this.getYPos(this.alarmParams.r_s*m_f)-this.getYPos(this.alarmParams.r_e*m_f));
	this.ctx.fillStyle=getColorString("yellow");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.y_e*m_f),this.width,this.getYPos(this.alarmParams.y_s*m_f)-this.getYPos(this.alarmParams.y_e*m_f));
	this.ctx.fillStyle=getColorString("green");
	this.ctx.fillRect(x,this.getYPos(this.alarmParams.g_e*m_f),this.width,this.getYPos(this.alarmParams.g_s*m_f)-this.getYPos(this.alarmParams.g_e*m_f));
 	


	}





		}

		this.ctx.font="bold 12px Georgia";
		this.ctx.fillStyle="black";



		/*
		var img = new Image();
		img.onload = function() {
			this.ctx=canvas.getContext("2d");
		    this.ctx.drawImage(img, 0, 0,this.width,this.height);
		}
		img.src = "img/grid.svg";
		*/

		this.ctx.beginPath();
		//this.ctx.shadowColor = color;

		//this.ctx.shadowBlur = 0.005;
		this.ctx.strokeStyle = color;
		this.ctx.lineJoin="round";
		this.ctx.lineCap='round';
		this.ctx.lineWidth=1;
		this.ctx_alert.strokeStyle = color;
		this.ctx_alert.fillStyle = color;
		
		this.ctx.moveTo(0, (height-this.originY));
		this.lastPoint.x=0;
		this.lastPoint.y=height;

		this.total = 0;

		this.stroke_buffer = strokeBuffer;
		this.stroke_count = 0;
		this.start_timestamp= start_timestamp;
		this.timestamp = 0;
		this.zero_timestamp = 0;
		

		//Mouse Over Tracker 

		
		 
		
	
}
CanvasChart.prototype.reset = function(){
		
		this.start_timestamp=0;
		this.timestamp=0;
		this.ctx.beginPath();

		this.ctx_y.beginPath();
		this.ctx_x.beginPath();
		this.ctx_alert.beginPath();
		this.ctx_alarm.beginPath();
		this.ctx_x_caption.beginPath();
		this.ctx_y_caption.beginPath();




		this.ctx.moveTo(0, (this.height-this.originY));
		this.firstPoint=true;
		this.lastPoint.x=0;
		this.lastPoint.y=(this.height-this.originY);

		this.total = 0;

		
		this.stroke_count = 0;
		
		this.timestamp = 0;
		this.zero_timestamp = 0;
		this.clear();

}
CanvasChart.prototype.getAll = function(){

	



	var can3 = document.createElement('canvas');
	can3.width  = this.width;
    can3.height = this.height;
	var ctx3 = can3.getContext('2d');
	ctx3.drawImage(this.canvas,0,0);
	ctx3.drawImage(this.canvas_alarm,0,0);
	ctx3.drawImage(this.canvas_alert,0,0);
	ctx3.drawImage(this.canvas_y,0,0);
	ctx3.drawImage(this.canvas_x,0,0);
	ctx3.drawImage(this.canvas_x_caption,0,0);
	ctx3.drawImage(this.canvas_y_caption,0,0);
	
	return can3;


}
function getMousePos(canvas, evt) {
        var rect =  canvas.getBoundingClientRect();
        return {
          x: evt.clientX - rect.left,
          y: evt.clientY - rect.top
        };
      }


CanvasChart.prototype.update_start_timestamp= function( new_start_timestamp ){
	this.start_timestamp = new_start_timestamp;
}


CanvasChart.prototype.add_point= function( t , y){

	if( t > this.start_timestamp && t > this.timestamp ){
		if(this.zero_timestamp == 0)
			this.zero_timestamp = t-this.baseShift_x;
		this.timestamp = t;


		var x = parseInt(t);
		if(this.firstPoint){

this.firstPoint=false;

this.ctx.moveTo(  this.getXPos(x) , this.getYPos(y)  );
	this.lastPoint.x=this.getXPos(x);
	this.lastPoint.y=this.getYPos(y);
}
			//this.ctx.lineCap="round";
			this.stroke_count = this.stroke_count + 1;
				//this.ctx.lineTo( ( x/this.max_x)*this.width ,this.height - ( ( y/(this.max_y) )*this.height ) );
			 // this.ctx.quadraticCurveTo(this.getXPos(x), this.getYPos(y), midPoint.x, midPoint.y);
			//this.ctx.lineTo( this.getXPos(x) , this.getYPos(y) );
			
			this.ctx.quadraticCurveTo(this.lastPoint.x,this.lastPoint.y,(this.lastPoint.x+this.getXPos(x))/2,(this.lastPoint.y+this.getYPos(y))/2);
			this.lastPoint.x=this.getXPos(x);
			this.lastPoint.y=this.getYPos(y);

				//console.log("w " + ( x/this.max_x)*this.width + " , " +  ( y/(this.height - this.max_y) )*this.height );
				if(this.stroke_count > this.stroke_buffer){
					this.ctx.stroke();
					this.stroke_count = 0;
					}
		
	//console.log(this.alarmContainer);
		/*if(this.alarmContainer.status){
				//console.log("Length Good, Sending Alarm "+this.alarmContainer[0]);

				
				this.alarm(t,"",this.alarmContainer.color);

		}*/
					


	}
		
}

CanvasChart.prototype.getYPos=function(y){

	return (this.height - ( ( y/(this.max_y) )*this.height )-30);
}
CanvasChart.prototype.getXPos=function(x){

	return (((x/this.max_x)*this.width)+25);
}


CanvasChart.prototype.clear = function(c){
	this.total = 0 ;
	this.ctx.clearRect( 0 , 0 , this.width , this.height);
	this.ctx_alarm.clearRect( 0 , 0 , this.width , this.height);
	this.ctx_alert.clearRect( 0 , 0 , this.width , this.height);
	
	console.log("My Dims are "+this.ctx_y.width+" "+this.ctx_y.height);
	this.ctx_y.clearRect( 0 , 0, this.width, -this.height);
	this.ctx_x.clearRect( 0 , 0 , this.width , this.height);
	this.ctx_y_caption.clearRect( 0 , 0, this.width, -this.height);
	this.ctx_x_caption.clearRect( 0 , 0 , this.width , this.height);
}


CanvasChart.prototype.alert_y = function(t, alertText,alarmColor){


		
			this.ctx_alert.beginPath();
			var x = parseInt(t);
			this.ctx_alert.font = "13px Georgia";
			this.ctx_alert.strokeStyle=alarmColor;
			this.ctx_alert.fillStyle=alarmColor;
			var width= this.ctx_alert.measureText(alertText).width;
			//this.ctx_alert.fillText(alertText, x+width, this.height/3);

			drawTextBG(this.ctx_alert,alertText,"13px Georgia",this.getXPos(x) , this.height/2 , alarmColor);
			this.ctx_alert.lineWidth = 1;

			this.ctx_alert.moveTo(this.getXPos(x) , this.height);
			this.ctx_alert.lineTo(this.getXPos(x) ,0  );
			this.ctx_alert.stroke();
		


		
}




CanvasChart.prototype.alert_x = function(y, alertText,alarmColor){
	
			this.ctx_alert.beginPath();
			this.ctx_alert.font = "13px Georgia";
			
			//alarmColor
			this.ctx_alert.fillStyle = alarmColor;
			
			
			pos_y=this.height - y;	
			var x = 25;
			this.ctx_alert.fillStyle=alarmColor;
			//this.ctx_alert.fillText(alertText,this.getXPos(x) , this.getYPos(y)+10);
			
			drawTextBG(this.ctx_alert,alertText,"13px Georgia",this.getXPos(x) , this.getYPos(y),alarmColor);

			this.ctx_alert.lineWidth = 2;
			this.ctx_alert.strokeStyle=alarmColor;
			
			this.ctx_alert.moveTo(x,this.getYPos(y));
		    this.ctx_alert.lineTo(this.width ,this.getYPos(y));


			this.ctx_alert.stroke();


					
		
}



function getColorString(alarmColor){

switch (alarmColor){
	case "yellow":
	return "rgba(255, 255, 0, 0.25)";
	break;
	case "red":
	return "rgba(255, 0, 0, 0.25)";
	break;
	case "green":
	return "rgba(0, 255, 0, 0.25)";
	break;
	case "":
	return "rgba(255, 255, 255, 0)";
	break;

}



}

CanvasChart.prototype.scale = function(X,Y, titleX , titleY){


	for(var i =0 ; i <= X ; i+=(X/10))
			this.ctx_x.fillText( parseInt(i).toString()  , (i  *this.width/X), this.height - 10);
	for(var i =0 ;i <= Y; i+=(Y/10)){
		
			this.ctx_y.fillText( parseFloat(i).toString()  , (this.height -  i  *this.height/Y)-this.originY, 0);	
		}
			
			this.ctx_y_caption.fillText(titleY , 10, -10);	

			this.ctx_x_caption.fillText( titleX , this.width/2, this.height - 20);
			this.ctx_x.lineWidth=2;
			this.ctx_x.moveTo(0,this.height - 30);
			this.ctx_x.lineTo(this.width,this.height - 30);

			this.ctx_x.stroke();

			this.ctx_y.lineWidth=2;
			this.ctx_y.moveTo(0,-25);
			this.ctx_y.lineTo(this.height,-25);
			this.ctx_y.stroke();

}

function drawTextBG(ctx, txt, font, x, y,color) {

    /// lets save current state as we make a lot of changes        
    ctx.save();

    /// set font
    ctx.font = font;
    txt= txt.trim();

    /// draw text from top - makes life easier at the moment
    ctx.textBaseline = 'top';

    /// color for background
    ctx.fillStyle = color;

    /// get width of text
    var width = ctx.measureText(txt).width;

    /// draw background rect assuming height of font
    ctx.fillRect(x-4, y, width+6, parseInt(font, 10)+4);

    /// text color
    ctx.fillStyle = 'black';

    /// draw text on top
    ctx.fillText(txt, x, y);

    /// restore original state
    ctx.restore();
}
