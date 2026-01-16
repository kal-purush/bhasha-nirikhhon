const button = document.getElementById("enter");
const input = document.getElementById("userinput");
let ul = document.querySelector("ul");
let listItems = document.getElementsByTagName("LI");
let deleteBtn = document.getElementsByTagName("button");
let deleteBtn2 = document.getElementById("clickMe");

$(function () {
  $('[data-toggle="popover"]').popover()
})

 listLength = () => {
 	return listItems.length
 }

for (i = 0; i < listLength(); i++ ) {
    let btn = document.createElement("button");
 		    btn.appendChild(document.createTextNode("//"));
 		    listItems[i].appendChild(btn);
 		    btn.onclick = removeParent;
 		}

removeParent = (evt) => {
	evt.target.removeEventListener("click", removeParent, false);
	evt.target.parentNode.remove();
}

deleteBtnsLength () => {
	return deleteBtn.length
}

checked = (event) => {
	if (event.target.tagName === "LI") {
		event.target.classList.toggle("done");
	}
}

inputLength = () => {
	return input.value.length;
}

createListElement = () => {

   let li = document.createElement("li");
   li.appendChild(document.createTextNode(input.value));
   ul.appendChild(li);
   input.value = "";

   let btn = document.createElement("button");
		btn.appendChild(document.createTextNode("//"));
 		li.appendChild(btn); 
    btn.onclick = removeParent;
 		deleteBtn2.style.visibility = "inherit";
}

addListAfterClick = () => {
   if (inputLength() > 0) {
   	createListElement();
  }
}

addListAfterKeypress = (event) => {
   if (inputLength() > 0 && event.keyCode === 13) {
    createListElement();
 }
}

deleteAll = () => {
	$('li').remove();
	deleteBtn2.style.visibility = "hidden";
};

deleteBtn2.style.visibility = "hidden";
ul.addEventListener("click", checked);
button.addEventListener("click", addListAfterClick);
input.addEventListener("keypress", addListAfterKeypress);
deleteBtn2.addEventListener("click", deleteAll);

