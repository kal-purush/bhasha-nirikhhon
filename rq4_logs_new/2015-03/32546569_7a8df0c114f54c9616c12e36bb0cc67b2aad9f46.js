function AutoSuggestControl(textBox, provider){
	this.layer = null;
	this.cur = -1;
	this.textbox = textBox;
	this.provider = provider;

	this.init();
}

AutoSuggestControl.prototype.generateNum = function (){
	var thisForm = document.forms['mySearch'];
	thisForm.elements["numResultsToSkip"].value = 0;
	thisForm.elements["numResultsToReturn"].value = 50;
};

AutoSuggestControl.prototype.createDropDown = function() {
	this.layer = document.createElement("div");
	this.layer.className = "suggestions";
	this.layer.style.visibility = "hidden";
	this.layer.style.width = this.textbox.offsetWidth;
	var oThis = this;
	this.layer.onmouseup = this.layer.onmousedown = this.layer.onmouseover = function (oEvent) {
		oEvent = oEvent || window.event;
		var oTarget = oEvent.target || oEvent.srcElement;
		if (oEvent.type == "mouseup"){
			oThis.textbox.focus();
		} else if (oEvent.type == "mousedown"){
			oThis.textbox.value = oTarget.firstChild.nodeValue;
			oThis.hideSuggestion();
		} else if (oEvent.type == "mouseover"){
			oThis.highlightSuggestion(oTarget);
		}
	};
	document.body.appendChild(this.layer);
};

AutoSuggestControl.prototype.getLeft = function() {
	var left = 0;
	var curNode = this.textbox;
	while (curNode.tagName != "BODY"){
		left += curNode.offsetLeft;
		curNode = curNode.offsetParent;
	}
	return left;
};

AutoSuggestControl.prototype.getTop = function() {
	var top = 0;
	var curNode = this.textbox;
	while (curNode.tagName != "BODY"){
		top += curNode.offsetTop;
		curNode = curNode.offsetParent;
	}
	return top;
};

AutoSuggestControl.prototype.showSuggestions = function(suggestions) {
	var newDiv = null;
	this.layer.innerHTML = "";
	this.cur = -1;
	var count = suggestions.length;
	if (count > 5){
		count = 5;
	}
	for (var i = 0; i < count; ++i){
		newDiv = document.createElement("div");
		newDiv.appendChild(document.createTextNode(suggestions[i]));
		this.layer.appendChild(newDiv);
	}
	this.layer.style.left = this.getLeft() + "px";
	this.layer.style.top = (this.getTop() + this.textbox.offsetHeight) + "px";
	this.layer.style.visibility = "visible";
};

AutoSuggestControl.prototype.hideSuggestion = function(){
	this.layer.style.visibility = "hidden";
	this.cur = -1;
};

AutoSuggestControl.prototype.highlightSuggestion = function(theNode){
	for (var i = 0; i < this.layer.childNodes.length; ++i){
		if (this.layer.childNodes[i] == theNode){
			this.layer.childNodes[i].className = "current";
			this.cur = i;
		} else if (this.layer.childNodes[i].className == "current"){
			this.layer.childNodes[i].className = "";
		}
	}
};

AutoSuggestControl.prototype.selectRange = function (start, len){
	if (this.textbox.createTextRange) {
		var range = this.textbox.createTextRange();
		range.moveStart("character", start);
		range.moveEnd("character", len - this.textbox.value.length);
		range.select();
	} else if (this.textbox.setSelectionRange) {
		this.textbox.setSelectionRange(start, len);
	}
	this.textbox.focus();
};

AutoSuggestControl.prototype.typeAhead = function (suggestion) {
	if (this.textbox.createTextRange || this.textbox.setSelectionRange){
		var start = this.textbox.value.length;
		this.textbox.value = suggestion;
		this.selectRange(start, suggestion.length);
	}
};

AutoSuggestControl.prototype.autosuggest = function (suggestions, fill) {
	if (suggestions.length > 0){
		if (fill){
			this.typeAhead(suggestions[0]);
		}
		this.showSuggestions(suggestions);
	} else {
		this.hideSuggestion();
	}
};

AutoSuggestControl.prototype.handleKeyUp = function (oEvent) {
	var key = oEvent.keyCode;
	if ((key == 8) || (key == 46)){
		this.provider.getSuggestion(this, false);
	} else if ((key < 32) || (key >= 33 && key < 46) || (key >= 112 && key <= 123)){
	} else {
		this.generateNum();
		this.provider.getSuggestion(this, true);
	}
};

AutoSuggestControl.prototype.handleKeyDown = function (oEvent) {
	var key = oEvent.keyCode;
	switch (key){
		case 38:
			this.previousSuggestion();
			break;
		case 40:
			this.nextSuggetion();
			break;
		case 13:
			this.hideSuggestion();
			break;
	}
};

AutoSuggestControl.prototype.previousSuggestion = function(){
	var sNodes = this.layer.childNodes;
	if (sNodes.length > 0 && this.cur > 0){
		var curNode = sNodes[--this.cur];
		this.highlightSuggestion(curNode);
		this.textbox.value = curNode.firstChild.nodeValue;
	}
};

AutoSuggestControl.prototype.nextSuggetion = function() {
	var sNodes = this.layer.childNodes;
	if (sNodes.length > 0 && this.cur < sNodes.length - 1){
		var curNode = sNodes[++this.cur];
		this.highlightSuggestion(curNode);
		this.textbox.value = curNode.firstChild.nodeValue;
	}
};

AutoSuggestControl.prototype.init = function (){
	var oThis = this;
	this.textbox.onkeyup = function(oEvent) {
		if (!oEvent){
			oEvent = window.event;
		}
		oThis.handleKeyUp(oEvent);
	};
	this.textbox.onkeydown = function(oEvent) {
		if (!oEvent){
			oEvent = window.event;
		}
		oThis.handleKeyDown(oEvent);
	};
	this.textbox.onblur = function() {
		oThis.hideSuggestion();
	};
	this.createDropDown();
};