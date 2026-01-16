(function() {
	var octave = 5;
	var audioCtx = new (window.AudioContext || window.webkitAudioContext)();
	var noteList = [];
	var keyList = [];
	var permArr = [];
	var usedChars = [];
	var fired = false;

	var mappedKeys = {
		65: { order:1, keyboard:'a',noteName: 'c', frequency: 32.7032 },
		87: { order:2, keyboard:'w',noteName: 'c#', frequency: 34.6478 },
		83: { order:3, keyboard:'s',noteName: 'd', frequency: 36.7081 },
		69: { order:4, keyboard:'e',noteName: 'd#', frequency: 38.8909 },
		68: { order:5, keyboard:'d',noteName: 'e', frequency: 41.2034 },
		70: { order:6, keyboard:'f',noteName: 'f', frequency: 43.6535 },
		84: { order:7, keyboard:'t',noteName: 'f#', frequency: 46.2493 },
		71: { order:8, keyboard:'g',noteName: 'g', frequency: 48.9994 },
		89: { order:9, keyboard:'y',noteName: 'g#', frequency: 51.9131 },
		72: { order:10, keyboard:'h',noteName: 'a', frequency: 55.0000 },
		85: { order:11, keyboard:'u',noteName: 'a#', frequency: 58.2705 },
		74: { order:12, keyboard:'j',noteName: 'b', frequency: 61.7354 },
		75: { order:13, keyboard:'k',noteName: 'c', frequency: 65.4064 },
		79: { order:14, keyboard:'o',noteName: 'c#', frequency: 69.2957 },
		76: { order:15, keyboard:'l',noteName: 'd', frequency: 73.4162 },
		80: { order:16, keyboard:'p',noteName: 'd#', frequency: 77.7817 },
		186: { order:17, keyboard:';',noteName: 'e', frequency: 82.4069 },
		222: { order:18, keyboard:"'",noteName: 'f', frequency: 87.3071 }
	};

	function createKeyboard(keys,oct) {
		var sorted = [];
		document.getElementById('keyboard').innerHTML = "";

		for(var keyCode in keys) {
			var freq = double(keys[keyCode].frequency, oct);
			var name = keys[keyCode].noteName;
			var keyboardKey = keys[keyCode].keyboard;
			keys[keyCode].key = new Key(freq, name, keyboardKey);
			sorted.push(keys[keyCode]);
		}

		var list = sorted.sort(function(a,b){
			return a.order - b.order;
		});

		list.forEach(function(item){
			document.getElementById('keyboard').appendChild(item.key.html)
		});

	}

	function generateHTML(note,keyboardKey) {
		var div = document.createElement('div');
		if(note.slice(-1) === '#') {
			div.className="key black-key";
		}
		else {
			div.className="key white-key";
		}

		div.innerHTML = '<span>' + keyboardKey + '</span>';

		return div;
	}

	function double(rootFreq, oct) {
		var freq = rootFreq;
		for(var i = 0; i < oct - 1; i++) {
			freq = freq * 2;
		} 

		return freq;
	}

	function Key(frequency,name,keyboardKey) {
		var keySound = new Sound(frequency);
		var html = generateHTML(name,keyboardKey);

		return {
			sound:keySound,
			html:html
		}
	}

	function Sound(frequency) {
		this.osc = audioCtx.createOscillator();
		this.osc.type = 'triangle';
		this.osc.frequency.value = frequency;
		this.osc.start(0);
	}

	Sound.prototype.play = function() {
		this.osc.connect(audioCtx.destination);
	}

	Sound.prototype.stop = function() {
		this.osc.disconnect();
	}

	function changeOctave(changed) {
		if (changed === 88 && octave < 8) {
			octave++;
		}
		else if (changed === 90 && octave > 0) {
			octave--;
		}

		createKeyboard(mappedKeys,octave)
	}

	var play = function(e) {
		if (e.keyCode === 90 || e.keyCode === 88) {
			changeOctave(e.keyCode);
			return;
		}
		else if (mappedKeys[e.keyCode] && keyList.indexOf(e.keyCode) === -1) {
			var obj = mappedKeys[e.keyCode]
			obj.key.sound.play();
			obj.key.html.classList.add("pressed");
			keyList.push(e.keyCode);

		}

		updateNotes(keyList);
	}

	var stop = function(e) {
		if (mappedKeys[e.keyCode]) {
			var obj = mappedKeys[e.keyCode];

			obj.key.sound.stop();
			obj.key.html.classList.remove('pressed');

			keyList = keyList.filter(function(item){

				return item !== e.keyCode;
			});

		}
		updateNotes(keyList);
	}

	function updateNotes(list) {
		document.getElementById('chords').innerHTML = "";
		var ul = document.createElement('ul');
		var str = '';

		list.forEach(function(item){
			str += '<li>' + mappedKeys[item].noteName + '</li>'
		});

		ul.innerHTML = str;

		document.getElementById('chords').appendChild(ul)
	}

	function permute(input) {
		var i, ch;
		for (i = 0; i < input.length; i++) {
			ch = input.splice(i, 1)[0];
			usedChars.push(ch);
			if (input.length == 0) {
				permArr.push(usedChars.slice());
			}
			permute(input);
			input.splice(i, 0, ch);
			usedChars.pop();
		}
		return permArr;
	}

	window.addEventListener('keydown', play);
	window.addEventListener('keyup', stop);
	window.addEventListener('load', function() {
		createKeyboard(mappedKeys, octave);
	});
}());