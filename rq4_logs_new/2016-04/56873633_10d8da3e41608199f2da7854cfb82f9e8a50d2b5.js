var WIDTH = 800;
var HEIGHT = 600;
var PIXEL_WIDTH = 40;
var PIXEL_HEIGHT = 40;
var TOTAL_PIXELS_HOR = WIDTH / PIXEL_WIDTH;
var TOTAL_PIXELS_VERT = HEIGHT / PIXEL_HEIGHT;
var MAX_PIXELS = TOTAL_PIXELS_HOR * TOTAL_PIXELS_VERT;

var canvas = new fabric.StaticCanvas('canvas');
canvas.selection = false;
canvas.renderOnAddRemove = false;
canvas.stateful = false;


var ctx = new AudioContext();
var audio = document.getElementById('myAudio');
var audioDuration = audio.duration;
var barMaxWidth = $("#audio-progress").width();
var track = Math.floor((Math.random() * 7) + 1);

if (track == 1){
	audio.src = 'public/tracks/1.mp3'; audioDuration = 69.01551;
} else if (track == 2){
	audio.src = 'public/tracks/2.mp3'; audioDuration = 70.791837;
} else if (track == 3){
	audio.src = 'public/tracks/3.mp3'; audioDuration = 58.618776;
} else if (track == 4){
	audio.src = 'public/tracks/4.mp3'; audioDuration = 74.501224;
} else if (track == 5){
	audio.src = 'public/tracks/5.mp3'; audioDuration = 67.056327;
} else if (track == 6){
	audio.src = 'public/tracks/6.mp3'; audioDuration = 72.620408;
}else {
	audio.src = 'public/tracks/7.mp3'; audioDuration = 71.235918;
}

if (Dolby.checkDDPlus() === true){
	if (track == 1)
		audio.src = 'public/tracks/1_Dolby.mp4';
	else if (track == 2)
		audio.src = 'public/tracks/2_Dolby.mp4';
	else if (track == 3)
		audio.src = 'public/tracks/3_Dolby.mp4';
	else if (track == 4)
		audio.src = 'public/tracks/4_Dolby.mp4';
	else if (track == 5)
		audio.src = 'public/tracks/5_Dolby.mp4';
	else if (track == 6)
		audio.src = 'public/tracks/6_Dolby.mp4';
	else // 7
		audio.src = 'public/tracks/7_Dolby.mp4';
}

var audioSrc = ctx.createMediaElementSource(audio);
var analyser = ctx.createAnalyser();
var gainNode = ctx.createGain();

audioSrc.connect(analyser);
audioSrc.connect(ctx.destination);

audioSrc.connect(gainNode);
gainNode.connect(ctx.destination);

var frequencyData = new Uint8Array(analyser.frequencyBinCount);
var ind = Math.floor((Math.random() * 16) + 0);
var colors;
var cont = 1;

if (Math.floor((Math.random() * 2) + 0) == 0)
	colors = GCOLORS[ind];
else
	colors = GCOLORS_A[ind];

gainNode.gain.value = 1;

function setVolumen(val){
	gainNode.gain.value = val;	
}

function renderFrame() {
	requestAnimationFrame(renderFrame);

	analyser.getByteFrequencyData(frequencyData);

	var fd = frequencyData[0];

	if (fd > 0 && cont++ == 7){
		printBlockColor(colors, fd);
		cont = 1;
		$("#bar").width(audio.currentTime * barMaxWidth / audioDuration);
	}
}
audio.play();
renderFrame();


function printBlockColor(colors, fd){
	var scale;
	var coords = getCoords();
	var c = Math.floor((Math.random() * (colors.length)) + 0);

	if (fd < 50)
		scale = 1;
	else if (fd < 100)
		scale = 5;
	else if (fd < 125)
		scale = 6;
	else if (fd < 150)
		scale = 7;
	else if (fd < 175)
		scale = 8;
	else if (fd < 200)
		scale = 9;
	else //if (fd < 250)
		scale = 10;

	printPixel(coords[0], coords[1], colors[c], 1.5, scale);

}

function getCoords(){
	return [Math.floor((Math.random() * TOTAL_PIXELS_HOR) + 0), Math.floor((Math.random() * TOTAL_PIXELS_VERT) + 0)];
}

function printPixel(x, y, color, v, s){
	var pixel = new fabric.Rect({
		left: x * PIXEL_WIDTH,
		top: y * PIXEL_HEIGHT,
		fill: color,
		width: PIXEL_WIDTH,
		height: PIXEL_HEIGHT,
		selectable: false,
		hasControls: false,
		hasBorders: false,
		hasRotatingPoint: false,
		borderScaleFactor: 0,
		strokeWidth: 0
	});
	canvas.add(pixel);

	if (s == 1)
		canvas.renderAll();
	else
		animarPixel(pixel, v, s);
}

function animarPixel(pixel, v, s){
	_animarPixel(pixel, pixel.left, pixel.top, 1, false, v, s);
}

function _animarPixel(pixel, l, t, i, b, v, s) {
	setTimeout(function (){
		pixel.set({left: l - i, top: t - i, width: PIXEL_WIDTH + i*2, height: PIXEL_HEIGHT + i*2});
		canvas.renderAll();

		if (i != 0){
			if (i == s || b)
				_animarPixel(pixel, l, t, i-1, true, v, s);
			else
				_animarPixel(pixel, l, t, i+1, false, v, s);
		}
	}, i * v);
}


$("#save-picture").click(function(){
	var link = document.getElementById('save-picture');
	link.href = canvas.toDataURL();
	link.download = "mypixturel.png";
});