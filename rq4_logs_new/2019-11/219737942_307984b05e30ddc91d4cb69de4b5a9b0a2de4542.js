"use strict";

window.onload = startPage;

function startPage() {
	var button = document.getElementById("myBtn");

	button.onclick = generateModalBox;
}

function generateModalBox() {
	var modal;
	var modalContent;
	var span, h2, p;
	var body = document.body;

	modal = document.getElementById("myModal");
	
	if(modal != null) {
		modal.parentNode.removeChild(modal);
	}
	
	modal = createElementNode(body, "div");
	createAttributeNode(modal, "id", "myModal");
	createAttributeNode(modal, "class", "modal");

	modalContent = createElementNode(modal, "div");
	createAttributeNode(modalContent, "class", "modal-content");

	span = createElementNode(modalContent, "span");
	createAttributeNode(span, "class", "close");
	createTextNode(span, "&times;");

	h2 = createElementNode(modalContent, "h2");
	createTextNode(h2, "Modal title");

	p = createElementNode(modalContent, "p");
	createTextNode(p, "Modal text");

	window.onclick = function(event) {
		  closeModal(event);
	}
}

function closeModal(event) {
	let modalClick = document.getElementById("myModal");
	let spanClose = document.getElementsByClassName("close").item(0);

	if(modalClick == event.target || spanClose == event.target) {
 		modalClick.style.display = "none";
 	}
}

function createElementNode(parentNode, elementName) {
	var elementt;

	elementt = document.createElement(elementName);
	parentNode.appendChild(elementt);

	return elementt;
}

function createAttributeNode(elementt, attributeName, attributeValue) {
	var attrNode;

	attrNode = document.createAttribute(attributeName);
	attrNode.nodeValue = attributeValue;
	elementt.setAttributeNode(attrNode);
}

function createTextNode(elementt, textt) {
	var textNode;

	textNode = document.createTextNode(textt);
	elementt.innerHTML = textt;
}
