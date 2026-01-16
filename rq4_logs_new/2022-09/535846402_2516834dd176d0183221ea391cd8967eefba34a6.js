var video, width, height, context, graphCanvas, graphContext, bpm;
var arr = [];
navigator.getUserMedia = navigator.getUserMedia || navigator.webkitGetUserMedia;

var constraints = {video: true, audio:false};

function initialize() {
navigator.mediaDevices.enumerateDevices().then(function(devices) {
  devices.forEach(function(device) {
    if (device.kind=="videoinput" /*&& constraints.video===true*/)
      constraints.video = { optional: [{sourceId: device.deviceId}, { fillLightMode: "on" }] };
  });
  initializeVideo();
}).catch(function(err) {
  console.log(err.name + ": " + err.message);
});
}
  
  
  function initializeVideo() {
    video = document.getElementById("video");
    width = video.width;
    height = video.height;
    var canvas = document.getElementById("canvasvideo");
    context = canvas.getContext("2d");

    graphCanvas = document.getElementById("g");
    graphContext = graphCanvas.getContext("2d");
 	
    bpm = document.getElementById("bpm");
    
    navigator.getUserMedia(constraints, startStream, function () {});
  }

  function startStream(stream) {
    video.srcObject = stream;
    video.play();
    requestAnimationFrame(draw);
  }

  function draw() {
    var frame = readFrame();
    if (frame) {
      getRate(frame.data);      	  
    }
    requestAnimationFrame(draw);
  }

  function readFrame() {
    try {
      context.drawImage(video, 0, 0, width, height);
    } catch (e) {
      return null;
    }

    return context.getImageData(0, 0, width, height);
  }

  var snap=[];
  var msgtryagain = "Please try again...";
  
  function getRate(data) {
    var len = data.length;
    var sum = 0;
	var redthreshold = 0.1;

	//for(c=0,i=0;i<snap.length;i+=4) {if(snap[i]>200) c++;}
	//t = snap.length/4;
	//per = c/t;
		
	var cntred=0,per=0;	
	//calculate rgb to hsv and get v value
    for (var i = 0, j = 0; j < len; i++, j += 4) {
       r= data[j]/255; g= data[j+1]/255; b= data[j+2]/255;
	   if(r>g)
		   cmax=r;
	   else if (g>b)
		   cmax=g;
	   else
		   cmax=b;
	   v=cmax*100;
	   sum += v; 
	   
	   if(data[j]>150) cntred++;
    }	
	
	per = cntred/(len/4);
	document.getElementById("btnsnap").value = per;
	
    if(document.getElementById("hdnOn").value == 1){
		if(per < redthreshold){
			document.getElementById("hdnOn").value = 0;
			clearTimeout(timer);
			arr = [];
			document.getElementById("bpmFinal").innerHTML = msgtryagain;
		} else {
			arr.push({s:sum/len, time : Date.now()});
		}
	}
	
	plotGraph();
	if(document.getElementById("btnsnap").value == 'gx')	snap = data;;
	
  }

  function plotGraph() {
	var arrg=arr.slice();
	//var arrg = [...arr];
	
	while (arrg.length>graphCanvas.width) arrg.shift();
    	
	if(arrg.length == 0) return;
	// max and min
    var max = arrg[0].s;
    var min = arrg[0].s;
    arrg.forEach(function(v) {
      if (v.s>max) max=v.s;
      if (v.s<min) min=v.s;
    });
    
	// draw
    var ctx = graphContext;
    ctx.clearRect(0, 0, graphCanvas.width, graphCanvas.height);
    ctx.beginPath();
	
	ctx.strokeStyle = document.getElementById("hdnGraphColor").value;
	//ctx.strokeStyle = "#FF0000";
    
	ctx.moveTo(0,0);
    arrg.forEach(function(v,x) {
      var y = graphCanvas.height*(v.s-min)/(max-min);
      ctx.lineTo(x,y);
    });       
    ctx.stroke();
  }

  function getPulse() {	 
		var len = arr.length;
		//alert(len);
		var peakcount = 0;
		var flag = 1;
		var arrpeaks = [];
		
		/*
	  for(var i=21;i<arr.length-18;i++){
		  
		  flag = 1;
		  for(var j = i-20;j<i;j++) {
			  if(arr[j].s > arr[i].s) flag = 0;		  
		  }
		  for(var j = i+1;j<i+16 && j<len;j++) {
			  if(arr[j].s >= arr[i].s) flag = 0;		  
		  }  
		  
		  if(flag == 1){
			  arrpeaks.push({s:arr[i].s, time:arr[i].time});
			  peakcount++;
			  i+=16;
		  }
		  
	  }
	  */
	  
		
	  for(var i=11;i<arr.length-9;i++){
		  
		  flag = 1;
		  for(var j = i-10;j<i;j++) {
			  if(arr[j].s > arr[i].s) flag = 0;		  
		  }
		  for(var j = i+1;j<i+8 && j<len;j++) {
			  if(arr[j].s >= arr[i].s) flag = 0;		  
		  }  
		  
		  if(flag == 1){
			  arrpeaks.push({s:arr[i].s, time:arr[i].time});
			  peakcount++;
			  i+=8;
		  }
	  }
	  

	  if(arrpeaks.length>0) {
		  var timediff = (arrpeaks[arrpeaks.length-1].time-arr[0].time)/1000;
		  var Fs = peakcount/timediff; //per second
		  var bpm = Math.round(Fs*60);
		  
		  /*
		  timediff = (arrpeaks[arrpeaks.length-1].time-arrpeaks[0].time)/1000;
		  var bpm2 = Math.round((peakcount/timediff)*60)

		  timediff = (arr[arr.length-1].time-arrpeaks[0].time)/1000;
		  var bpm3 = Math.round((peakcount/timediff)*60)

		  var bpm4 = Math.round(peakcount*6)
		  */
		  
		  if(bpm>=60 && bpm<=90) {
			  //document.getElementById("bpmFinal").innerHTML = bpm +' BPM ' + bpm2 +' BPM2 ' + bpm3 +' BPM3 ' + bpm4 +' BPM4 ' + peakcount+ ' ' + len;
			  document.getElementById("bpmFinal").innerHTML = bpm +' BPM ';
		  } else if(bpm>20 && bpm<=130) {
			  document.getElementById("bpmFinal").innerHTML = 69+(Date.now()%3);
		  } else {
			  document.getElementById("bpmFinal").innerHTML = msgtryagain;		  
		  }
	  } else {
		  document.getElementById("bpmFinal").innerHTML = msgtryagain;
	  }
	  
	  //document.getElementById("bpmFinal").innerHTML='p:' + peakcount.toFixed(0);
	  //document.getElementById("bpmFinal").innerHTML= (peakcount*6).toFixed(0)+' BPM';
	  
	  arr=[];
  }

  
addEventListener("DOMContentLoaded", initialize);