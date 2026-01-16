hl.cityIDs = [];
hl.cities = ["Anchorage","Vancouver","Los Angeles","Edmonton","Denver","Mexico City","Dallas","Chicago","Winnipeg","Wahshington DC","New York","Toronto","Quebec","Lima","Bogota","Santiago","Manaus","Caracas","Buenos Aires","Sao Paulo","Rio De Janeiro","Brasilia","Casablanca","London","Kinshasa","Lagos","Algiers","Rome","Madrid","Paris","Stockholm","Cape Town","Johannesburg","Kisangani","Cairo","Nairobi","Addis Ababa","Khartoum","Riyadh","Baghdad","Istanbul","Moscow","Saint Petersburg","Tehran","Karachi","Astrakhan","Kabul","Yekaterinburg","Dhaka","Kolkata","Mumbai","New Delhi","Omsk","Astana","Krasnoyarsk","Novosibirsk","Rangoon","Bangkok","Ho Chi Minh City","Medan","Jakarta","Perth","Makassar","Manila","Taipei","Guangzhou","Shanghai","Beijing","Urumqi","Irkutsk","Alice Springs","Tokyo","Seoul","Yakutsk","Melbourne","Sydney","Brisbane","Yuzhno-Sakhalinsk","Vladivostok","Magadan","Auckland","Petropavlosk"];
hl.timezones = ["America/Anchorage","America/Vancouver","America/Los_Angeles","America/Edmonton","America/Denver","America/Mexico_City","America/Chicago","America/Chicago","America/Winnipeg","America/New_York","America/New_York","America/Toronto","America/Montreal","America/Lima","America/Bogota","America/Santiago","America/Manaus","America/Caracas","America/Buenos_Aires","America/Sao_Paulo","America/Sao_Paulo","America/Sao_Paulo","Africa/Casablanca","Europe/London","Africa/Kinshasa","Africa/Lagos","Africa/Algiers","Europe/Rome","Europe/Madrid","Europe/Paris","Europe/Stockholm","Africa/Johannesburg","Africa/Johannesburg","Africa/Johannesburg","Africa/Cairo","Africa/Nairobi","Africa/Addis_Ababa","Africa/Khartoum","Asia/Riyadh","Asia/Baghdad","Asia/Istanbul","Europe/Moscow","Europe/Moscow","Asia/Tehran","Asia/Karachi","Europe/Astrakhan","Asia/Kabul","Asia/Yekaterinburg","Asia/Dhaka","Asia/Kolkata","Asia/Kolkata","Asia/Kolkata","Asia/Omsk","Asia/Omsk","Asia/Krasnoyarsk","Asia/Novosibirsk","Asia/Rangoon","Asia/Bangkok","Asia/Ho_Chi_Minh","Asia/Jakarta","Asia/Jakarta","Australia/Perth","Asia/Makassar","Asia/Manila","Asia/Taipei","Asia/Shanghai","Asia/Shanghai","Asia/Shanghai","Asia/Urumqi","Asia/Irkutsk","Asia/Tokyo","Asia/Tokyo","Asia/Seoul","Asia/Yakutsk","Australia/Melbourne","Australia/Sydney","Australia/Brisbane","Asia/Vladivostok","Asia/Vladivostok","Asia/Magadan","Pacific/Auckland","Pacific/Auckland"];

/** doc ready function **/
hl.hermioneJS = function(){

	/* svg manipulation */
	document.querySelectorAll("g g").forEach(function(content){
		if(content.hasAttribute("id")){
			hl.cityIDs.push(content.id);
			//hl.cityList.push(content.id.split(/(?=[A-Z]+[^A-Z]?)/).join("_"));
			content.classList.add("city");
		  var title = document.createElementNS("http://www.w3.org/2000/svg","title");
		  title.textContent = content.id;
		  content.appendChild(title);
		}
	});
	/* remove all the fill:nones */
	document.querySelectorAll("path").forEach(function(path){
		if(path.hasAttribute("style")){
			path.removeAttribute("style");
		}
	});

	/* set clock variables */
	hl.secondHand = document.querySelector(".localClock .second-hand");
	hl.minsHand = document.querySelector(".localClock .min-hand");
	hl.hourHand = document.querySelector(".localClock .hour-hand");
	hl.secondHandTZ = document.querySelector(".tzClock .second-hand");
	hl.minsHandTZ = document.querySelector(".tzClock .min-hand");
	hl.hourHandTZ = document.querySelector(".tzClock .hour-hand");
	/* fill select box with cities and their timezones, sort cities alphabetically */
	hl.tzst = null;
	hl.selectTZ = document.getElementById("selectTZ");
	hl.selectTZ.addEventListener("change", hl.citySelected);
	hl.orderedCities = [];
	for (var i = 0; i<hl.cities.length; i++){
		hl.orderedCities.push([hl.cities[i], hl.timezones[i], hl.cityIDs[i]]);
	}
	hl.orderedCities.sort();
	const optPrompt = document.createElement('option');
  optPrompt.value = "none";
  optPrompt.innerHTML = "Please select a city";
	hl.selectTZ.appendChild(optPrompt);
	for (var i = 0; i<hl.orderedCities.length; i++){
	    var opt = document.createElement('option');
	    opt.value = hl.orderedCities[i][1];
	    opt.innerHTML = hl.orderedCities[i][0];
	    hl.selectTZ.appendChild(opt);
	}

	document.querySelectorAll(".city").forEach(function(city){
			city.addEventListener("click", hl.cityClicked);
	});
}

/** other functions **/
hl.cityClicked = function(event){
	var elementID = event.target.parentElement.id;
	var clickedElement = document.getElementById(elementID);
	var indexOfSelected = hl.cityIDs.indexOf(elementID)
	var timezone = hl.timezones[hl.selectTZ.selectedIndex];
	for(var j=0; j<hl.orderedCities.length; j++){
		if(hl.orderedCities[j][2]==elementID){
			var cityIndex = j;
		}
	}
	hl.selectTZ.selectedIndex = cityIndex+1;
	hl.timeZoneSelected(clickedElement, timezone);	
}
hl.citySelected = function(event){
	if(event.target.value != "none"){
		var selectedText = event.target.selectedOptions[0].textContent;
		var selectedElement = document.getElementById(hl.cityIDs[hl.cities.indexOf(selectedText)]);
		hl.timeZoneSelected(selectedElement, event.target.value);	
	}	
}
hl.timeZoneSelected = function(selectedElement, timezone){
		document.querySelectorAll(".city").forEach(function(city){
			city.classList.remove("selectedCity");
			city.parentElement.classList.remove("selectedTZ");
		});
		selectedElement.classList.add("selectedCity");
		selectedElement.parentElement.classList.add("selectedTZ");
		hl.chosenTZ = timezone;
		clearInterval(hl.tzst);
		hl.tzst = setInterval(hl.setTZTime, 1000);
}
hl.setLocalTime = function(){
	const now = new Date();
	//console.log(now)
	//console.log(now.getUTCHours())
	//console.log(now.getTimezoneOffset())
	const seconds = now.getSeconds();
	const secondDegrees = ((seconds/60)*360)+90;
	hl.secondHand.style.transform = `rotate(${secondDegrees}deg)`;
	const mins = now.getMinutes();
	const minDegrees = ((mins/60)*360)+90;
	hl.minsHand.style.transform = `rotate(${minDegrees}deg)`;
	const hours = now.getHours();
	const hourDegrees = ((hours/12)*360)+(mins/2)+90;
	hl.hourHand.style.transform = `rotate(${hourDegrees}deg)`;

	hl.checkForNightTime(hours, ".localClock");
}
hl.setTZTime = function(){
	const now = new Date();
	const tzNow = new Date(now.toLocaleString("en-US", {timeZone: hl.chosenTZ,  hour12: false}));
	const seconds = tzNow.getSeconds();
	const secondDegrees = ((seconds/60)*360)+90;
	hl.secondHandTZ.style.transform = `rotate(${secondDegrees}deg)`;
	const mins = tzNow.getMinutes();
	const minDegrees = ((mins/60)*360)+90;
	hl.minsHandTZ.style.transform = `rotate(${minDegrees}deg)`;
	const hours = tzNow.getHours();
	const hourDegrees = ((hours/12)*360)+(mins/2)+90;
	hl.hourHandTZ.style.transform = `rotate(${hourDegrees}deg)`;

	hl.checkForNightTime(hours, ".tzClock");
}
hl.checkForNightTime = function(hours, whichClock){
	const clock = document.querySelector(whichClock + " .clock");
	if(hours<6 || hours>17){
		clock.style.backgroundImage = "url('../images/javascript/stars.jpg')";
		document.querySelectorAll(whichClock + " .hand").forEach(function(hand){
			hand.style.backgroundColor = "white";
		});
	}else{
		clock.style.backgroundImage = "none";
		document.querySelectorAll(whichClock + " .hand").forEach(function(hand){
			hand.style.backgroundColor = "black";
		});
	}
}
setInterval(hl.setLocalTime, 1000);

