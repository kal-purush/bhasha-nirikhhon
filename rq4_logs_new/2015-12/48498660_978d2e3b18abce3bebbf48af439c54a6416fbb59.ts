/// <reference path="model.ts" />
/// <reference path="view.ts" />
/// <reference path="utils.ts" />
/// <reference path="soundUtils.ts" />

var canvas = <HTMLCanvasElement> document.getElementById("example")
var ctx = canvas.getContext("2d")

var audioCtx = new AudioContext();

SoundUtils.loadAll(Model.Keys.keySounds, audioCtx);
SoundUtils.loadAll(Model.Percussion.percSounds, audioCtx);
SoundUtils.loadAll(Model.Bass.bassSounds, audioCtx);
SoundUtils.loadAll(Model.Strings.stringSounds, audioCtx);
SoundUtils.loadAll(Model.Lead.leadSounds, audioCtx);

var TIME_SIG = new Model.TimeSignature(4, 4);
var bpm = 120
var currentInstrument = Model.ITypes[0];

enum KeyCodes {
	UNDO = 32,
	LEFT = 37,
	RIGHT = 39,
	UP = 38,
	DOWN = 40,
	DEC_BPM = 188,
	INC_BPM = 190
}

var control = new Model.Cc();

var genNewNote = function(event: MouseEvent) {
	var canvas_p = new Utils.Point(event.pageX, event.pageY);
	var r = new Model.Repeater(currentInstrument.type,
						 canvas_p,
						 currentInstrument.bar,
						 TIME_SIG,
						 bpm)
	control.addNote(r);
}

canvas.addEventListener("mouseup", genNewNote, false);

var keyPressed = function(event: KeyboardEvent) {
	var code = event.keyCode;

	switch (code) {
		case KeyCodes.UNDO:
			control.removeNote();
			break;
		case KeyCodes.LEFT:
			if (currentInstrument.id > 0)
				currentInstrument = Model.ITypes[currentInstrument.id - 1];
			break;
		case KeyCodes.RIGHT:
			if (currentInstrument.id < Model.ITypes.length - 1)
				currentInstrument = Model.ITypes[currentInstrument.id + 1];
			break;
		case KeyCodes.UP:
			currentInstrument.bar += 1;
			break;
		case KeyCodes.DOWN:
			if (currentInstrument.bar > 1)
				currentInstrument.bar -= 1;
			break;
		case KeyCodes.DEC_BPM:
			if (bpm > 1)
				bpm -= 1;
			control.setBpm(bpm);
			break;
		case KeyCodes.INC_BPM:
			bpm += 1
			control.setBpm(bpm)
	}
}

window.onkeydown = function (e) {
	return !(e.keyCode == 32) && !(e.keyCode == 38) && !(e.keyCode == 40);
}

window.addEventListener("keyup", keyPressed, false);

View.start();