(function () { "use strict";
var Main = function() { };
Main.main = function() {
	new Metronome();
};
var Metronome = function() {
	this.timerWorker = null;
	this.notesInQueue = [];
	this.last16thNoteDrawn = -1;
	this.noteLength = 0.1;
	this.noteResolution = 0;
	this.nextNoteTime = 0.0;
	this.scheduleAheadTime = 0.1;
	this.lookahead = 25.0;
	this.tempo = 120.0;
	this.isPlaying = false;
	this.init();
};
Metronome.prototype = {
	nextNote: function() {
		var secondsPerBeat = 60.0 / this.tempo;
		this.nextNoteTime += 0.25 * secondsPerBeat;
		this.current16thNote++;
		if(this.current16thNote >= 16) this.current16thNote = 0;
	}
	,scheduleNote: function(beatNumber,time) {
		this.notesInQueue.push({ note : beatNumber, time : time});
		if(this.noteResolution == 1 && beatNumber % 2 != 0) return;
		if(this.noteResolution == 2 && beatNumber % 4 != 0) return;
		var osc = this.audioContext.createOscillator();
		osc.connect(this.audioContext.destination,0,0);
		if(beatNumber % 16 == 0) osc.frequency.value = 880.0; else if(beatNumber % 4 == 0) osc.frequency.value = 440.0; else osc.frequency.value = 220.0;
		osc.start(time);
		osc.stop(time + this.noteLength);
	}
	,scheduler: function() {
		while(this.nextNoteTime < this.audioContext.currentTime + this.scheduleAheadTime) {
			this.scheduleNote(this.current16thNote,this.nextNoteTime);
			this.nextNote();
		}
	}
	,play: function() {
		this.isPlaying = !this.isPlaying;
		if(this.isPlaying) {
			this.current16thNote = 0;
			this.nextNoteTime = this.audioContext.currentTime;
			this.timerWorker.postMessage("start");
			return "stop";
		} else {
			this.timerWorker.postMessage("stop");
			return "play";
		}
	}
	,resetCanvas: function(e) {
		this.canvas.width = window.innerWidth;
		this.canvas.height = window.innerHeight;
		window.scrollTo(0,0);
	}
	,draw: function($float) {
		var currentNote = this.last16thNoteDrawn;
		var currentTime = this.audioContext.currentTime;
		while(this.notesInQueue.length > 0 && this.notesInQueue[0].time < currentTime) {
			currentNote = this.notesInQueue[0].note;
			this.notesInQueue.splice(0,1);
		}
		if(this.last16thNoteDrawn != currentNote) {
			var x = Math.floor(this.canvas.width / 18);
			this.canvasContext.clearRect(0,0,this.canvas.width,this.canvas.height);
			var _g = 0;
			while(_g < 16) {
				var i = _g++;
				if(currentNote == i) {
					if(currentNote % 4 == 0) this.canvasContext.fillStyle = "red"; else this.canvasContext.fillStyle = "blue";
				} else this.canvasContext.fillStyle = "black";
				this.canvasContext.fillRect(x * (i + 1),x,x / 2,x / 2);
			}
			this.last16thNoteDrawn = currentNote;
		}
		this.requestAnimFrame($bind(this,this.draw));
		return true;
	}
	,init: function() {
		var _g = this;
		window.document.getElementById("play").onmousedown = function(e) {
			_g.play();
		};
		window.document.getElementById("tempo").oninput = function(event) {
			_g.tempo = event.target.value;
			if(_g.tempo == null) window.document.getElementById("showTempo").innerHTML = "null"; else window.document.getElementById("showTempo").innerHTML = "" + _g.tempo;
		};
		window.document.getElementById("selResolution").onchange = function(event1) {
			_g.noteResolution = event1.target.selectedIndex;
		};
		var container;
		var _this = window.document;
		container = _this.createElement("div");
		container.className = "container";
		var _this1 = window.document;
		this.canvas = _this1.createElement("canvas");
		this.canvasContext = this.canvas.getContext("2d");
		this.canvas.width = window.innerWidth;
		this.canvas.height = window.innerHeight;
		window.document.body.appendChild(container);
		container.appendChild(this.canvas);
		this.canvasContext.strokeStyle = "#ffffff";
		this.canvasContext.lineWidth = 2;
		this.audioContext = new AudioContext();
		window.onresize = $bind(this,this.resetCanvas);
		this.requestAnimFrame = ($_=window,$bind($_,$_.requestAnimationFrame));
		this.requestAnimFrame($bind(this,this.draw));
		this.timerWorker = new Worker("js/metronomeworker.js");
		this.timerWorker.onmessage = function(e1) {
			if(e1.data == "tick") _g.scheduler(); else console.log("message: " + e1.data);
		};
		this.timerWorker.postMessage({ interval : this.lookahead});
	}
};
var $_, $fid = 0;
function $bind(o,m) { if( m == null ) return null; if( m.__id__ == null ) m.__id__ = $fid++; var f; if( o.hx__closures__ == null ) o.hx__closures__ = {}; else f = o.hx__closures__[m.__id__]; if( f == null ) { f = function(){ return f.method.apply(f.scope, arguments); }; f.scope = o; f.method = m; o.hx__closures__[m.__id__] = f; } return f; }
Math.NaN = Number.NaN;
Math.NEGATIVE_INFINITY = Number.NEGATIVE_INFINITY;
Math.POSITIVE_INFINITY = Number.POSITIVE_INFINITY;
Math.isFinite = function(i) {
	return isFinite(i);
};
Math.isNaN = function(i1) {
	return isNaN(i1);
};
Main.main();
})();