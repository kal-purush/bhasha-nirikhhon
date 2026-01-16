let xml;
let frames;
let container;
let count;

function preload() {
	xml = loadXML('./assets/frames.xml');
	music = createAudio('./assets/music.mp3');
}

function setup() {
	music.loop();
	frames = xml.getChildren();
	count = 0;
	frameRate(1);
}

function draw() {
	setArt(count);
	count += 1;
	if (count > frames.length - 1) {
		count = 0;
	}
}

function setArt(index) {
	container = document.getElementById("valentine");
	if (container != null) {
		container.innerHTML = "<pre>" + frames[index].DOM.textContent + "</pre>";
	}
}