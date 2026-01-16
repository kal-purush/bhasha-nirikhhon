window.onload = initForm;
window.onunload = function(){};

//When the window unloads (when it is closed or the browser goes to another location),
// we call an anonymous function, that is, a function that does not have a name.
// at this case, this function does not even do anything. we have it here is because we want to make sure the onload event 
// isn't triggered when the browser's back buttons is cliked (because the page is cached in some browsers, such as firefox and safari)

// Having onunload do nothingthing at ll causes the page to be uncached, and therefore, when we come back, the onload happens

//The anonymous part of the term

// refers to the fact that there’s no name between function and (). 
// This is the simplest way to trigger onunload but not have it do anything. 
// The braces are just like any function; they would hold the contents of the function. 
// They’re empty here because this particular function does nothing.

// onchange	The event occurs when the content of a form element, 
// 			the selection, or the checked state have changed 
// 			(for <input>, <keygen>, <select>, and <textarea>)

function initForm(){
	document.getElementById("BinGo").selectedIndex = 0;
	document.getElementById("BinGo").onchange = pageChanage;
}

function pageChanage(){
	debugger;
	var selecObject = document.getElementById("BinGo");
	var newPgae = selecObject.options[selecObject.selectedIndex].value;

	if(newPgae != ""){
		window.location = newPgae;
		//In other words, if newPage has a value, 
		//then tell the window to go to the URL specified by the menu option chosen.
	}
}




















