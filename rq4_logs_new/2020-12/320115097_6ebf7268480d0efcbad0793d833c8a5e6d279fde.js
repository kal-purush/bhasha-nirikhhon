let ModelViewerLoaded = false;
let ThumpnailClicked = -1;
let LastThumpnailClicked = -1;
let fullscreen = false;
let ProjectIndex = 0;
let viewport = document.getElementById("viewer3d");
let loadBtn = document.getElementById("loadBtn");
let viewerImg = document.getElementById("viewerImg");
let fullscreenBtn = document.getElementById("fullscreenBtn");
let loadingSpinner = document.getElementById("loadingSpinner");
let loadingBar = document.getElementById("loadingBar");
let webGLViewer = document.getElementById("webGLViewer");
let modelViewer = document.getElementById("modelViewer");
let poster = document.getElementById("poster");
let title = document.getElementById('title');
let specs = document.getElementById('specs');
let description = document.getElementById('description');
let date = document.getElementById('date');
let size = document.getElementById('size');
let textureRez = document.getElementById('textureRez');
let verts = document.getElementById('verts');
let faces = document.getElementById('faces');
let mats = document.getElementById('mats');
let additionalOptionsContainer = document.getElementById('additionalOptionsContainer');
let imgs; // 2D Array
let imgLoaded;

Image.prototype.completedPercentage = 0;

let ModelProjects;
fetch('3DProjects/3DProjects.json')
.then(response => response.json())
.then(data => ModelProjects = data)
.then(ModelProjects => {
	imgs = new Array(ModelProjects.length);
	imgLoaded = new Array(ModelProjects.length);
	ModelProjects.forEach(prepareProjects); 

});

Image.prototype.load = async function(url){
	let thisImg = this;
	let xmlHTTP = new XMLHttpRequest();
	xmlHTTP.open('GET', url, true);
	xmlHTTP.responseType = 'arraybuffer';
	xmlHTTP.onloadstart = function() {
		thisImg.completedPercentage = 0;
	};
	xmlHTTP.onprogress = function(e) {
		thisImg.completedPercentage = parseInt((e.loaded / e.total) * 100);
	};
	xmlHTTP.onload = function(e) {
		let blob = new Blob([this.response]);
		thisImg.src = window.URL.createObjectURL(blob);
	};
	xmlHTTP.send();
};

loadBtn.addEventListener("click", function() {ModelViewerLoaded = true;});
fullscreenBtn.addEventListener("click", fullscreenToggle)

async function modelProjectManager(clicked) {
	if(hasValue(ModelProjects)) {
		fadeIn(webGLViewer);
		
		// Clear/Reset
		fadeOut2(title);
		fadeOut2(description);
		fadeOut2(date);
		fadeOut2(size);
		fadeOut2(textureRez);
		fadeOut2(verts);
		fadeOut2(faces);
		fadeOut2(mats);
		fadeOut2(additionalOptionsContainer);
		title.innerHTML = "";
		description.innerHTML = "";
		date.innerHTML = "";
		size.innerHTML = "";
		textureRez.innerHTML = "";
		verts.innerHTML = "";
		faces.innerHTML = "";
		mats.innerHTML = "";
		additionalOptionsContainer.innerHTML = "";
		fadeOut2(specs);

		if(LastThumpnailClicked != -1) {
			fadeOut(imgs[ProjectIndex][LastThumpnailClicked]);
		}
		fadeIn(modelViewer);
		fadeIn(loadBtn);

		ThumpnailClicked = -1;
		LastThumpnailClicked = -1;
		ModelViewerLoaded = false; // Future update, don't reload the viewer, use fadein/fadeout

		ProjectIndex = findIndex(clicked);

		poster.style.backgroundImage = `url('3DProjects/${ModelProjects[ProjectIndex].folder}/images/thumbnails/${ModelProjects[ProjectIndex].thumbnails[0]}')`;
		console.log(ModelProjects[ProjectIndex].thumbnails[0]);

		if(hasValue(ProjectIndex)) {
			modelViewer.src =  `3DProjects/${ModelProjects[ProjectIndex].folder}/${ModelProjects[ProjectIndex].model}`;
			await Promise.all([
				setTimeout(() => startTypeOut(title, ModelProjects[ProjectIndex].projectTitle, 30), 300),
				setTimeout(() => fadeIn2(specs), 619),
				setTimeout(() => startTypeOut(description, ModelProjects[ProjectIndex].description, 1), 600),
				setTimeout(() => startTypeOut(date, ModelProjects[ProjectIndex].date, 30), 620),
				setTimeout(() => startTypeOut(size, ModelProjects[ProjectIndex].size, 30), 620),
				setTimeout(() => startTypeOut(textureRez, ModelProjects[ProjectIndex].textureRez, 30), 620),
				setTimeout(() => startTypeOut(verts, ModelProjects[ProjectIndex].verts, 30), 620),
				setTimeout(() => startTypeOut(faces, ModelProjects[ProjectIndex].faces, 30), 620),
				setTimeout(() => startTypeOut(mats, ModelProjects[ProjectIndex].mats, 30), 620)
			]);

			setTimeout(fadeIn(additionalOptionsContainer), 301);
			additionalOptionsContainer.style.display = "flex"
			function addAdditionalOptions(z = 0) {
				if(z < ModelProjects[ProjectIndex].thumbnails.length) {
					let link = document.createElement('a');
					let option = document.createElement('img');
					link.href = "#webGLViewer";
					link.appendChild(option)
					additionalOptionsContainer.appendChild(link);
					option.src = `3DProjects/${ModelProjects[ProjectIndex].folder}/images/thumbnails/${ModelProjects[ProjectIndex].thumbnails[z]}`;
					option.classList.add("clickable");
					option.onclick = () => clickManager(z - 1);
					fadeIn(option)
					z++
					setTimeout(() => addAdditionalOptions(z), 50);
				}
			}
			setTimeout(() => addAdditionalOptions(), 400);
		}
	}
	else {
		setTimeout(modelProjectManager(clicked), 100);
	}
}

function hasValue(x) {
	if(x !== undefined && x !== null && x !== '') {
		return true;
	} else {
		return false;
	}
}

function findIndex(name) {
	for (let i = 0; i < ModelProjects.length; i++) {
		if (name === ModelProjects[i].uniqueName) {
			return i;
		}
	}
	throw "Index not found.";
}

function clickManager(number) {
	if(number == 'next' && LastThumpnailClicked < imgs[ProjectIndex].length - 1) {
		ThumpnailClicked = LastThumpnailClicked + 1;
	}
	else if(number == 'previous' && LastThumpnailClicked > -1) {
		ThumpnailClicked = LastThumpnailClicked - 1;
	}
	if(number != 'next' && number != 'previous') {
		ThumpnailClicked = number - 1;
	}
	CanvasManager(ThumpnailClicked);
	loadingManager(ThumpnailClicked);
	LastThumpnailClicked = ThumpnailClicked;
}

function CanvasManager(clicked) {
	if(clicked === -1) {
		if(ModelViewerLoaded) {
			fadeIn(modelViewer);
		}
		else {
			fadeIn(loadBtn);
		}
	}
	else {
		if(ModelViewerLoaded) {
			fadeOut(modelViewer);
		}
		fadeOut(loadBtn);
	}
}

function loadingManager(clicked) {
	for(let i = 0; i < imgs[ProjectIndex].length; i++)	{
		let img = imgs[ProjectIndex][i];
		if(i === clicked) {
			fadeIn(img);
		}
		else {
			fadeOut(img);
		}
	}

	if(!imgLoaded[ProjectIndex][clicked] && clicked >= 0){
		loadAndAddImage(viewerImg, clicked);
		imgLoaded[ProjectIndex][clicked] = true;
	}
}

function prepareProjects(item, projIndex, ary) {
	imgs[projIndex] = new Array(item.length);
	imgLoaded[projIndex] = new Array(item.length);
	item.images.forEach((innerItem, imageIndex) => prepareDisplayImgs(innerItem, imageIndex, projIndex));
}

function prepareDisplayImgs(item, imageIndex, projIndex) {
	imgs[projIndex][imageIndex] = new Image ();
}

function loadAndAddImage(imgContainer, number) {
	let img = imgs[ProjectIndex][number];
	img.load(`3DProjects/${ModelProjects[ProjectIndex].folder}/images/${ModelProjects[ProjectIndex].images[number]}`);
	
	let promiseToLoadImg = new Promise((resolve) => {
		let progress = 5;
		fadeIn(loadingBar);
		fadeIn(loadingSpinner);
		let checkProgress = setInterval(frame, 10);
		function frame() {
			progress = img.completedPercentage;
			if (progress >= 100) {
				clearInterval(checkProgress);
				fadeOut(loadingBar);
				fadeOut(loadingSpinner);
				resolve();
			} else {
				loadingBarUpdate(progress);
			}
		}
	});
	promiseToLoadImg.then(() => {
		fadeIn(img);
		imgContainer.appendChild(img);
	});	
}

function fullscreenToggle() {
	if (fullscreen) {
		fullscreenClose();
		fullscreen = false
	}
	else {
		fullscreenOpen();
		fullscreen = true;
	}
}

function fullscreenOpen() {
	let elem = document.documentElement;
	if (elem.requestFullscreen) {
		elem.requestFullscreen();
	} else if (elem.mozRequestFullScreen) { /* Firefox */
		elem.mozRequestFullScreen();
	} else if (elem.webkitRequestFullscreen) { /* Chrome, Safari & Opera */
		elem.webkitRequestFullscreen();
	} else if (elem.msRequestFullscreen) { /* IE/Edge */
		elem.msRequestFullscreen();
	}
	viewport.classList.add("fullscreenViewer3d");
	viewport.style.width = `${window.innerWidth}px`;
	viewport.style.minWidth = "100%";
	viewport.classList.remove("defautViewer3D");
	fullscreenBtn.style.position = "fixed";
}

function fullscreenClose() {

	function fullscreencloseHelper() {
		viewport.classList.add("defautViewer3D");
		viewport.style.width = "";
		viewport.style.minWidth = "";
		viewport.classList.remove("fullscreenViewer3d");
		fullscreenBtn.style.position = "absolute";
	}

	if (document.exitFullscreen) {
		fullscreencloseHelper();
		document.exitFullscreen();
	} else if (document.mozCancelFullScreen) {
		fullscreencloseHelper();
		document.mozCancelFullScreen();
	} else if (document.webkitExitFullscreen) {
		fullscreencloseHelper();
		document.webkitExitFullscreen();
	} else if (document.msExitFullscreen) {
		fullscreencloseHelper();
		document.msExitFullscreen();
	}
	else {
		fullscreencloseHelper();
	}
}

function loadingBarUpdate(width) {
	loadingBar.style.width = `calc(${width}% - 32px - 10px`;
}

async function startTypeOut(elm, txt, speed, i = 0) { 
	fadeIn(elm);
	if(txt.length < 50)
		setTimeout(async () => await typeOut(elm, txt, speed, i), speed);
	else
		elm.innerHTML = txt;
}

async function typeOut(elm, txt, speed, i) {
	if (i < txt.length) {
		elm.innerHTML += txt.charAt(i);
		i++;
		setTimeout(async () => await typeOut(elm, txt, speed, i), speed);
	}
}

function fadeOut(obj){
	obj.style.opacity = 0;
	obj.classList.remove("fadeOut");
	if(obj.classList.contains("fadeIn")) {
		obj.classList.add("fadeOut");
		obj.classList.remove("fadeIn");
		setTimeout(() => {obj.style.display = "none";}, 300);
	}
};

function fadeIn(obj){
	obj.style.display = "block";
	obj.style.opacity = 1;
	obj.classList.add("fadeIn");
	obj.classList.remove("fadeOut");
};

function fadeOut2(obj){
	obj.style.opacity = 0;
	obj.classList.remove("fadeOut");
	if(obj.classList.contains("fadeIn")) {
		obj.classList.add("fadeOut");
		obj.classList.remove("fadeIn");
	}
};

function fadeIn2(obj){
	obj.style.opacity = 1;
	obj.classList.add("fadeIn");
	obj.classList.remove("fadeOut");
};

function vary(speed) {
	return Math.floor(Math.random() * ((speed + 30) - (speed - 30)) ) + (speed - 30);
  }