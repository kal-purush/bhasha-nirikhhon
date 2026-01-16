var importantTag = document.getElementById("important");
importantTag.style.color = "#F00";

var pTags = document.getElementsByTagName("p");
for (var i = 0; i < pTags.length; i++) {
	pTags[i].style.backgroundColor = "#FF0";
}

var specialTags = document.getElementsByClassName("special");
for (var i = 0; i < specialTags.length; i++) {
	specialTags[i].style.backgroundColor = "#00F";
}

var differentTags = document.querySelectorAll("body h1, a");
for (var i = 0; i < differentTags.length; i++) {
	differentTags[i].style.backgroundColor = "#0F0";
}

document.getElementById("important").innerHTML = "<b>Some</b>thing...";

var aTags = document.querySelectorAll("a");
for (var i = 0; i < aTags.length; i++) {
	aTags[i].setAttribute("href", "https://google.com");
}

document.getElementById("inputText").value = "Something...";

window.addEventListener("load", function(){ console.log("loaded"); });
document.addEventListener("DOMContentLoaded", function(){ console.log("DOMContentLoaded"); });

var h1Tag = document.querySelectorAll("h1");
for (var i = 0; i < h1Tag.length; i++) {
	h1Tag[i].addEventListener("click", h1Click);
}
function h1Click()
{
	this.style.backgroundColor = "#F00";
}

document.getElementById("inputSubmit").addEventListener("click", submitClick);
function submitClick(e)
{
	e.preventDefault();
	e.target.style.backgroundColor = "#F00";
}


fetch("https://reqres.in/api/users/2")
	.then(function (response) {
		if (response.status !== 200) {
			console.log("Problem occured... Status Code: " + response.status);
			return;
		}
		response.json().then(function (data) {
			console.log(data);
			console.log(data.data.avatar);
			document.getElementById("imgJson").setAttribute("src", data.data.avatar);
		});
	})
	.catch(function (error) {
		console.log("Error occured...", error);
	});