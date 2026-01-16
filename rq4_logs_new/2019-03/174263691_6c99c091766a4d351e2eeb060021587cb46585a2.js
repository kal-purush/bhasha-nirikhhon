// https://kylemcdonald.github.io/cv-examples/
// https://github.com/kylemcdonald/AppropriatingNewTechnologies/wiki/Week-2

var capture;
var tracker
var w = 640,
    h = 480;

var zeroPosition = 0,
	fourteenPosition = 0;

var ball = {};
var soundFile1, soundFile2;

function preload() {
  soundFormats('mp3', 'ogg');
  soundFile1 = loadSound('sound1.mp3');
  soundFile2 = loadSound('sound2.mp3');
}

function setup() {
    capture = createCapture({
        audio: false,
        video: {
            width: w,
            height: h
        }
    }, function() {
        console.log('capture ready.')
    });
    capture.elt.setAttribute('playsinline', '');
    createCanvas(w, h);
    capture.size(w, h);
    capture.hide();

    colorMode(HSB);

    tracker = new clm.tracker();
    tracker.init();
    tracker.start(capture.elt);
}

function draw() {
    image(capture, 0, 0, w, h);
    var positions = tracker.getCurrentPosition();
	
	// Full Face
	noFill();
	stroke(255);
	beginShape();
	for (var i = 0; i < positions.length - 56; i++) {
		if (i == 0 ) { zeroPosition = positions[i][1]; }
		if (i == 14 ) { fourteenPosition = positions[i][1]; }
		vertex(positions[i][0], positions[i][1]);
	}
	endShape();
	
	if ( zeroPosition <= fourteenPosition + 20 && zeroPosition >= fourteenPosition - 20 ) {
		// Center
		console.log('Center');
	} else if ( zeroPosition >= fourteenPosition) {
		// Right
		console.log('Right');
		if (!soundFile1.isPlaying()){
        soundFile1.play();}
	} else if ( zeroPosition <= fourteenPosition ) {
		// Left
		console.log('Left');
        if (!soundFile2.isPlaying()){
		soundFile2.play();}
	}
	
	// Eyebrow Left
	noFill();
	stroke(255);
	beginShape();
	for (var i = 19; i < positions.length - 48; i++) {
		vertex(positions[i][0], positions[i][1]);
	}
	endShape();
	
	// Eyebrow Right
	noFill();
	stroke(255);
	beginShape();
	for (var i = 15; i < positions.length - 52; i++) {
		vertex(positions[i][0], positions[i][1]);
	}
	endShape();
	
	// Mouth
	noFill();
	stroke(255);
	beginShape();
	for (var i = 44; i < positions.length - 15; i++) {
		vertex(positions[i][0], positions[i][1]);
		if ( i == positions.length - 16 ) { vertex(positions[44][0], positions[44][1]); }
	}
	endShape();
	
	// Nose
	noFill();
	stroke(255);
	beginShape();
	for (var i = 33; i < positions.length - 30; i++) {
		vertex(positions[i][0], positions[i][1]);
		if ( i == positions.length - 31 ) { vertex(positions[33][0], positions[33][1]); }
	}
	endShape();
	
	// Left Eye
	noFill();
	stroke(255);
	beginShape();
	for (var i = 23; i < positions.length - 44; i++) {
		vertex(positions[i][0], positions[i][1]);
		if ( i == positions.length - 45 ) { vertex(positions[23][0], positions[23][1]); }
	}
	endShape();
	
	// Right Eye
	noFill();
	stroke(255);
	beginShape();
	for (var i = 28; i < positions.length - 39; i++) {
		vertex(positions[i][0], positions[i][1]);
		if ( i == positions.length - 40 ) { vertex(positions[28][0], positions[28][1]); }
	}
	endShape();
}