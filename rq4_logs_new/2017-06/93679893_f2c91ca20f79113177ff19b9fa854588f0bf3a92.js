"use strict";

var vid = document.getElementById("bbb-vid"),
	currentVidRate = vid.playbackRate,
	displayRate = document.getElementById('current-speed'),
	subValue = 0.5,
	addValue = 1;


function displayVidRate(){
	displayRate.innerHTML = "Current Video Speed: " + currentVidRate;
}

function slowVidRate(){
	vid.playbackRate = vid.playbackRate - subValue;
	currentVidRate = vid.playbackRate;
	displayVidRate()
}

function fasterVidRate(){
	vid.playbackRate = vid.playbackRate + addValue;
	currentVidRate = vid.playbackRate;
	displayVidRate()
}

$(document).ready(function(){
	displayVidRate();

	$('#slower').click(function(){
		if(currentVidRate === 0){
			displayRate.innerHTML = "You are already at 0 speed. Please click Speed Up.";
		}
		else{
			slowVidRate();
		}
	});

	$('#faster').click(function(){
		fasterVidRate();
	});

});



