// ==UserScript==
// @name         LeMonde
// @namespace    @ludovicm67
// @version      1.0.0
// @description  Clean LeMonde
// @author       @ludovicm67
// @include      *.lemonde.fr/**
// @grant        none
// ==/UserScript==
'use strict';

// We remove the counter (15 free articles/month)
localStorage.removeItem('us_meter_readed');


// We remove some useless elements
var uselessElems = document.querySelectorAll('.dfp_slot, .meter_toaster');
for(var i=0; i<uselessElems.length; i++) {
    uselessElems[i].parentNode.removeChild(uselessElems[i]);
}
window.setTimeout(function () {
	var warning = document.querySelector('.js_meter_toaster');
	if(warning) warning.parentNode.removeChild(warning);
	else window.setTimeout(function () {
		var warning = document.querySelector('.js_meter_toaster');
		if(warning) warning.parentNode.removeChild(warning);
	}, 200);
}, 200);

// We adjust some content
function adjustContents() {
	var adjElems = document.querySelectorAll('article, .omega');
	for(var i=0; i<adjElems.length; i++) {
	    adjElems[i].style.textAlign = 'justify';
	}
}
adjustContents();
window.setTimeout(adjustContents, 700);