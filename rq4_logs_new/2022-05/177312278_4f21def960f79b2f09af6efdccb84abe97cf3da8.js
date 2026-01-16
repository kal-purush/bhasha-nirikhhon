/* 
	The Engine, by Terminal Illness
*/

var keyListen = {
	start : function() {
		window.onkeyup = function(e) {
			let key = e.keyCode;
			if (ASCII_KEYS.includes(key)) {
				keyListen.keys.splice(keyListen.keys.indexOf(key), 1);
				if (keyListen.keypresses.includes(key)) {
					keyListen.keypresses[keyListen.keypresses.indexOf(key)+1]();
				}
			}
		};
		window.onkeydown = function(e) {
			let key = e.keyCode;
			if (!keyListen.keys.includes(key) && ASCII_KEYS.includes(key)) {
				keyListen.keys.push(key);
			}
		};
	},
	keys : [],
	keypresses : [],
	scrollWheelOff : function() {
		window.onwheel = function() {
		    return false;
		};
	}
};
var ASCII_KEYS = [
	"A", 65, "B", 66, "C", 67,
	"D", 68, "E", 69, "F", 70,
	"G", 71, "H", 72, "I", 73,
	"J", 74, "K", 75, "L", 76,
	"M", 77, "N", 78, "O", 79,
	"P", 80, "Q", 81, "R", 82,
	"S", 83, "T", 84, "U", 85,
	"V", 86, "W", 87, "X", 88,
	"Y", 89, "Z", 90, "a", 97,
	"b", 98, "c", 99, "d", 100,
	"e", 101, "f", 102, "g", 103,
	"h", 104, "i", 105, "j", 106,
	"k", 107, "l", 108, "m", 109,
	"n", 110, "o", 111, "p", 112,
	"q", 113, "r", 114, "s", 115,
	"t", 116, "u", 117, "v", 118,
	"w", 119, "x", 120, "y", 121,
	"z", 122, "ENTER", 13
];
function toASCII(character) {
	for (let i = 0; i < ASCII_KEYS.length; i += 2) {
		if (character == ASCII_KEYS[i]) {
			return ASCII_KEYS[i+1];
		}
	}
	return -1;
}
function removePressListener(key) {
	keyListen.keypresses.splice(keyListen.keypresses.indexOf(toASCII(key)), 2);
}
function addPressListener(key, todo) {
	if (!keyListen.keypresses.includes(toASCII(key))) {
		keyListen.keypresses.push(toASCII(key));
		keyListen.keypresses.push(todo);
	}
}
function getKeyDown(key) {
	if (keyListen.keys.includes(toASCII(key))) {
		return true;
	}
	return false;
}
function RandomInt(min, max) {
	this.gen = function(min, max) {
		this.val = Math.floor(Math.random() * ((max+1) - min) + min);
	};
	this.gen(min, max);
}
function decide(probability) {
	if (new RandomInt(0, 100).val >= probability) {
		return true;
	} else {
		return false;
	}
}