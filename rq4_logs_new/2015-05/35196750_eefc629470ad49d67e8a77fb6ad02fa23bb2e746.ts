import * as player from "./player";

var midiPlayer: player.Player;

document.addEventListener("DOMContentLoaded", (e) => {
    document.querySelector("#playButton").addEventListener("click", (e) => {
		let req = new XMLHttpRequest();
		req.open("GET", "./midi/test.mid", true);
		req.responseType = "arraybuffer";
		req.onload = (e) => {
			console.log(e);
			var buffer = req.response;
			if (buffer) {
				midiPlayer = new player.Player(buffer);
				(<any>window).midiPlayer = midiPlayer;
				midiPlayer.channels[0].on("noteon", (e) => {
					console.log(e);
				});
				midiPlayer.play();
			}
		};
		req.send(null);
	});	
});