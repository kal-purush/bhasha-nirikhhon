// JavaScript Document



  //Glossary toggle
function displayGloss() {
	if (document.getElementById("glossary").style.display === "none") {
		document.getElementById("glossary").style.display = "block";
	}
	else {
		document.getElementById("glossary").style.display = "none";
	}
}


//Pagination
var spot = 0;  
function pagination(nextPrev) {	
	
	var number = document.getElementsByName("content");
	var page = document.getElementsByName("title");
	var length = page.length;
	
	
	if (nextPrev === -1 || nextPrev === 0 ) {
		if (nextPrev === 0) {
			nextPrev = 1;
		}

		if ((spot + nextPrev) < number.length && (spot + nextPrev) >= 0 ) {
			spot += nextPrev;
			number[spot].click();
		}

		else if ((spot + nextPrev) === (number.length -1)) {
			spot = number.length;
		}

		else if ((spot + nextPrev) === 0) {
			spot = 0;
		}
	}
	
	else {
		nextPrev--;
		spot = nextPrev;
		
	}
	//Phone pagination
	document.getElementById("pageNumber").innerHTML = spot + 1;
	
	//Completion bubble
        //Find percentage and fill up heart image
        var bar1 = new ldBar("#percent");
		var level = (((spot + 1) / number.length) * 100);
        bar1.set(level);
      
}
////End Pagination



function hit() {
	document.getElementById("query").innerHTML = "";
}

function numberOfPages() {
	"use strict";
	let link = document.location.href;
	let page = document.getElementsByName("title");
	let href = document.getElementsByName("content");
	let number = page.length;
	let pageNumber = (link.indexOf("#") + 1);
	var link2 = location.pathname;
	var start = (link2.indexOf("HTML") + 5);
	var end = link2.indexOf("_");	
	var place = link2.slice(start, end);

//	document.getElementById("numberOf").innerHTML = number;
//	href[0].click();
	
	 var toolKitOnOrOff = localStorage.getItem("switchPosition");
		if (toolKitOnOrOff === "true") {
		document.getElementById("toolkitNav").style.width = "20vw";
		document.getElementById("toolkitNav").style.paddingLeft = "10px";
		document.getElementById("toolkitNav").style.paddingRight = "10px";
		document.getElementById("content").style.paddingRight = "150px";
		
			
		document.getElementById('switch').checked = true;
		document.getElementById("switchMode").innerHTML = "ON";
		document.getElementById("switchMode").style.color = "#2D92E0";
		document.getElementById("toolTab").style.paddingLeft = "20vw";
	}
	
	
		else {
			document.getElementById("toolkitNav").style.width = "0vw";
			document.getElementById("toolkitNav").style.paddingLeft = "0px";
			document.getElementById("toolkitNav").style.paddingRight = "0px";
			document.getElementById("switchMode").innerHTML = "OFF";
			document.getElementById("switchMode").style.color = "#999999";
		}
	
	
	
	onPageLoad();
	
	
	//get the unit number to store in local storage so that
	// the videos for the different units will appear
	
	var path = parseInt(link.indexOf("HTML")) + 5;
	var unit = parseInt(link.charAt(path));
	localStorage.setItem("unitNum", unit);
	
	
	var retNotes = localStorage.getItem(place);
	document.getElementById("notes").innerHTML = retNotes;
	
//	localStorage.clear();
	
}
	

//Toolbar Section
//Glossary
function wordList() {
	
	if (document.getElementById("wordTool").style.display === "none") {
		document.getElementById("wordTool").style.display = "block";
	}
	else {
		document.getElementById("wordTool").style.display = "none";
	}
	document.getElementById("h5p").focus();
	document.getElementById("forvoTool").style.display = "none";
}

//Forvo
function forvo() {
	
	if (document.getElementById("forvoTool").style.display === "none") {
		document.getElementById("forvoTool").style.display = "block";
	}
	else {
		document.getElementById("forvoTool").style.display = "none";
	}
	document.getElementById("spanish").focus();
	document.getElementById("wordTool").style.display = "none";
}

function search() {
			let word = document.getElementById("spanish").value;
			document.getElementById("iframe_A").src = "https://forvo.com/word/" + word +"/#es";
//			document.getElementById("iframe_A").style.borderWidth = .5;			
//			document.getElementById("spanish").value = "";
		}
function selectAll() {	
			document.getElementById("spanish").focus();
			document.getElementById("spanish").select();
}

function searchWord(word) {
		document.getElementById("spanish").value = word;
		search();
//		document.getElementById("spanish").value = word;
		}



// Verbs Tool - JavaScript
function openNav(source) {
  document.getElementById("verbs").style.width = "100%";
  document.body.style.backgroundColor = "rgba(0,0,0,0.4)";
  document.getElementById("verbs").style.display = "block";
  document.getElementById("toolNav").src = source;
}

function closeNav() {
  document.getElementById("verbs").style.width = "0";
  document.body.style.backgroundColor = "white";
}

//Toolkit Open/Close
function openTools() {
	var test = document.getElementById("toolkitNav").style.width;
	
	if (test === "0vw") {
		document.getElementById("toolkitNav").style.width = "20vw";
		document.getElementById("toolkitNav").style.paddingLeft = "10px";
		document.getElementById("toolkitNav").style.paddingRight = "10px";
		document.getElementById("toolkit").focus();
		document.getElementById("toolTab").style.paddingLeft = "20vw";		
		document.getElementById("content").style.paddingRight = "150px";
	}	
	
	
	else {
		closeTools();		
		document.getElementById("content").style.paddingRight = "0px";
	}
}

function closeTools() {
	var page = document.getElementsByClassName("page");
	document.getElementById("toolkitNav").style.width = "0vw";
	document.getElementById("toolkitNav").style.paddingLeft = "0";
	document.getElementById("toolkitNav").style.paddingRight = "0";	
	document.getElementById("toolTab").style.paddingLeft = "0px";
	
	for (var i = 0; i < page.length; i++ ) {
			page[i].style.paddingRight = "15px";
			page[i].style.maxWidth = "700px";
		}
}

//Swiping (Touch)
document.addEventListener('touchstart', handleTouchStart, false);        
document.addEventListener('touchmove', handleTouchMove, false);
var xDown = null;                                                        
var yDown = null;  

function handleTouchStart(evt) {                                         
    xDown = evt.touches[0].clientX;                                      
    yDown = evt.touches[0].clientY;                                      
}; 

function handleTouchMove(evt) {
    if ( ! xDown || ! yDown ) {
        return;
    }
    var xUp = evt.touches[0].clientX;                                    
    var yUp = evt.touches[0].clientY;
    var xDiff = xDown - xUp;
    var yDiff = yDown - yUp;

    if ( Math.abs( xDiff ) > Math.abs( yDiff ) ) {/*most significant*/
        if ( xDiff > 0 ) {
        /* left swipe */
			nextPage();
        } else {
        /* right swipe */
			prevPage();
        }                       
    } else {
        if ( yDiff > 0 ) {
        /* up swipe */ 
        } else { 
        /* down swipe */
        }                                                                 
    }
    /* reset values */
    xDown = null;
    yDown = null;                                             
};
//End Swiping


//Phone Tool Kit
function phoneToolKit() {
  document.getElementById("phoneToolKit").style.height = "100%";
}

function closeee() {
  document.getElementById("phoneToolKit").style.height = "0%";
}

function phoneWordList() {
	document.getElementById("phoneTools").innerHTML = "<p class='iframeA'><iframe src='https://byu.h5p.com/content/1290692211000459188/embed' width='1088' height='300vw' frameborder='0' allowfullscreen='allowfullscreen' allow='geolocation *; microphone *; camera *; midi *; encrypted-media *'></iframe><script src='https://byu.h5p.com/js/h5p-resizer.js' charset='UTF-8'></script></p>";
}

function phoneForvo() {
	document.getElementById("phoneTools").innerHTML = "<p class='iframeA'><label for='spanish' style='color: black'>Spanish text: </label><textarea id='spanish' rows='1' cols='30'></textarea><br><br><a class='button' onClick='search()' id='srchButton'>Search</a><br><br><iframe src='' width='100%' height='300vw' overflow='hidden' frameborder='0' id='iframe_A'></iframe></p>";
	document.getElementById("spanish").focus();
}

function alwaysOn() {
	
//	var checkbox = document.getElementById('switch');
//	if (checkbox.checked === true) {
//		localStorage.setItem('switch', checkbox.checked = true);
//	}
//	else {
//		localStorage.setItem('switch', checkbox.checked = false);
//	}
//	var checked = JSON.parse(localStorage.getItem('switch'));
//	if (checked === true) {
//	}
	
	var checked = document.getElementById('switch');
	if (checked.checked === true) {
		var page = document.getElementsByClassName("page");
		document.getElementById("toolkitNav").style.width = "20vw";
		document.getElementById("toolkitNav").style.paddingLeft = "10px";
		document.getElementById("toolkitNav").style.paddingRight = "10px";
		
		document.getElementById("switchMode").innerHTML = "ON";
		document.getElementById("switchMode").style.color = "#2D92E0";		
		document.getElementById("content").style.paddingRight = "0px";
	}
	else {
		checked.checked === false;
		document.getElementById("switchMode").innerHTML = "OFF";
		document.getElementById("switchMode").style.color = "#999999";
		closeTools();
	}
	
	localStorage.setItem("switchPosition", checked.checked);
}

//
//
//	if (document.getElementById("switch") === "checked") {
//		var page = document.getElementsByClassName("page");
//		document.getElementById("toolkitNav").style.width = "20vw";
//		document.getElementById("toolkitNav").style.paddingLeft = "10px";
//		document.getElementById("toolkitNav").style.paddingRight = "10px";
//		document.getElementById("toolkit").focus();
//
//		for (var i = 0; i < page.length; i++ ) {
//			page[i].style.paddingRight = "22vw";
//		}
//	}


function videoAndH5P(videoCode, start, end, h5pCode) {
	var URL = "https://www.youtube.com/embed/" + videoCode + "?autohide=1&modestbranding=1&color=white&showinfo=0&rel=0&start=" + start + "&end=" + end + ";&autoplay=1";
	
	document.getElementsByName("myFrame")[0].src = URL;
	
	document.getElementById("H5P").src = "https://byu.h5p.com/content/" + h5pCode + "/embed";
}


function saveNotes() {
	"use strict";
	var link = location.pathname;
	var start = (link.indexOf("HTML") + 5);
	var end = link.indexOf("_");	
	var place = link.slice(start, end);
	var notes = document.getElementById("notes").value;
	localStorage.setItem(place, notes);
}

function getStudentGoal() {
		"use strict";

  var x = document.getElementById("studentGoal").value;
  document.getElementById("demo").innerHTML = x;
}