//variables
var xhr;
const delayInMinutes = 0;
const periodInMinutes = 1;
//function defs
function getImageData() {
  var canvas = document.createElement("canvas");
  var ctx = canvas.getContext("2d");

  ctx.fillStyle = "green";
  ctx.fillRect(10, 10, 100, 100);
 
  return ctx.getImageData(50, 50, 100, 100);
}

function setIconCaqi(caqi){
	  if(caqi < 25) color="#30B32D";
	  else if(caqi < 50) color="#F0DD00"; 
	  else if(caqi < 75) color="#FF0000"; 
	  else if(caqi < 100) color="#ae00ff"; 
	  else if (caqi > 125) color="#ae00ff";
	  else {
			color="#aeaeae"; 
			caqi="?";
	  }
	  if (caqi!="?") {
		 fontsize=100-Math.ceil(Math.log(caqi)+1)*6 
		 caqi= Math.round(caqi,1)
	  
	  }
	  else fontsize=100;
	  console.log(caqi)
		var canvas = document.createElement("canvas");
		var ctx = canvas.getContext("2d");         
		ctx.fillStyle = color;
		ctx.fillRect(0, 0, 110, 110);			
		ctx.font=(fontsize+ "px Arial");
		ctx.fillStyle = "black";
		ctx.textAlign = "center";
		ctx.fillText(caqi,60,90);
		console.log(caqi)
		browser.browserAction.setIcon({imageData: ctx.getImageData(10, 10, 100, 100)});
}
getMeasurements();
function onloadfunc(){
		  console.log(xhr.responseText)
          response=(JSON.parse(xhr.responseText));
          console.log(xhr.responseText)
          caqi=response.airQualityIndex;
          setIconCaqi(caqi);
}
function getMeasurements(){
  function makeRequest(lat,lon){
	  xhr = new XMLHttpRequest();
	  console.log('https://airapi.airly.eu/v1/nearestSensor/measurements?maxDistance=10000&latitude=' + lat+'&longitude='+lon)
	  xhr.open('get', 'https://airapi.airly.eu/v1/nearestSensor/measurements?maxDistance=10000&latitude=' + lat+'&longitude='+lon, true);
	  xhr.setRequestHeader('apikey', '8896a4dc8ed84466bbd9cc7d8fff9942');
	  xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
	  xhr.onloadend=onloadfunc;
	  xhr.send();
  }
  function decide(settings){
	  console.log(settings)
	  if(settings.mangeo.mode=="manual"){
		  makeRequest(settings.mangeo.lat,settings.mangeo.lon);
	  }
	  else{
		  navigator.geolocation.getCurrentPosition(
			  //on success
		  function(position) {
			 makeRequest(position.coords.latitude,position.coords.longitude);
			  //on error
		}, function(position){
				var canvas = document.createElement("canvas");
				var ctx = canvas.getContext("2d");         
				ctx.fillStyle = "#aeaeae";
				ctx.fillRect(10, 10, 100, 100);			
				ctx.font="100px Arial";
				ctx.fillStyle = "black";
				ctx.textAlign = "center";
				ctx.fillText("?",60,90);
				browser.browserAction.setIcon({imageData: ctx.getImageData(10, 10, 100, 100)});

	})
  
	  }
		  
  }
  var getting = browser.storage.local.get("mangeo");
  getting.then(decide, function(){console.log("error in Airly Plugin!") } );

  
}

//webextensions api calls
browser.alarms.create('updatePollution', 
	{ delayInMinutes,
	periodInMinutes
})
browser.alarms.onAlarm.addListener(() => {
	console.log("aaa")
    getMeasurements()
});