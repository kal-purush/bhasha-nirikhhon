const target = "https://kartavyaxmeme.herokuapp.com/memes";
const container = document.getElementById("container");
const n1 = document.getElementById("n1");
const u1 = document.getElementById("u1");
const c1 = document.getElementById("c1");

function openPanel() {
	var mySpan = document.getElementById("p1");
	mySpan.style.width = "30%";
	mySpan.style.padding = "2% 5% 2% 2%";
}

function closePanel() {
	var mySpan = document.getElementById("p1");
	mySpan.style.width = "0";
	mySpan.style.padding = "0";
}

function togglenform() {
	var nform = document.getElementById("newMeme");
	if(nform.style.height === "0px") {
		nform.style.height = "50%";
	} else {
		nform.style.height = "0";
	}
}

var meme_id;

var editForm = document.getElementById("patchForm");

function closewin() {
	editForm.style.display = "none";
}

function loadForm(id) {
	meme_id = id;
	editForm.style.display = "block";
}

function fetchFullData() {
	fetch(target).then((res) => {
		return res.json();
	}).then((data) => {
		data.forEach((meme) => {
			container.insertAdjacentHTML("afterbegin", 
				`<div class="meme">
					<button class="edit" style="float: right;" onclick="loadForm('${meme._id}')"><img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTm_ejkBqA8WWNyPM3dEmRJr-C3yRRoU_08QA&usqp=CAU" style="height: 30px; width: 30px;" id='${meme._id}'></button>
					<center><p class="cap">"${meme.caption}"</p>
					<img src="${meme.url}" alt="${meme.caption}"></center>
					<p  class="uname">Added By: ${meme.name}</p>
				 </div>`);
		});
	}).catch((err) => {
		window.alert(err);
	});
}

editForm.addEventListener('submit', (e) => {
	e.preventDefault();
	var formData = new FormData(e.currentTarget);
	var value = Object.fromEntries(formData.entries());
	if(value.url === "") {
		delete value.url;
	}
	if(value.caption === "") {
		delete value.caption;
	}
	fetch(target+`/${meme_id}`, {
		method: 'PATCH',
		body: JSON.stringify(value),
		headers: {
			'Content-Type': 'application/json'
		}
	}).then((res) => {
		container.innerHTML="";

	}).catch((err) => {
		window.alert(error);
	});
	document.querySelectorAll('.out').forEach((query) => {
		query.value="";
	});
	closewin();
});

var newForm = document.getElementById("newForm");

newForm.addEventListener('submit', (event) => {

	event.preventDefault();

	const formData = new FormData(event.currentTarget);
	var value =  Object.fromEntries(formData.entries());
	console.log(value);
	fetch(target, {
		method: 'POST',
		body: JSON.stringify(value),
		headers: {
          'Content-Type': 'application/json'
        }
	}).then((res) => {
		container.innerHTML = "";
		fetchFullData();
	}).catch((err) => {
		window.alert("Error: " + err);
	});
	n1.value = "";
	u1.value = "";
	c1.value = "";
});

fetchFullData();