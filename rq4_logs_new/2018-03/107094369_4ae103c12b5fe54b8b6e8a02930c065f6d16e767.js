
//let x = document.getElementsByClassName("listen");

let bplaypause ;
let song = music1;
bplaypause = document.getElementById("playpause");

bplaypause.addEventListener("click",playAudio);


let em1 = document.getElementById("m1");
let em2 = document.getElementById("m2");
let em3 = document.getElementById("m3");
let em4 = document.getElementById("m4");


em1.addEventListener("click",fsong1);
em2.addEventListener("click",fsong2);
em3.addEventListener("click",fsong3);
em4.addEventListener("click",fsong4);


function fsong1(){
    song = music1;
    return song;
}

function fsong2(){
    song = music2;
    return song;
}

function fsong3(){
    song = music3;
    return song;
}

function fsong4(){
    song = music4;
    return song;
}



function playAudio() {

	if (song.paused) {
        music1.pause();
        music2.pause();
        music3.pause();
        music4.pause();
        song.play();
        playpause.className = "";
		playpause.className = "pause";
	} else{
        song.pause();
        playpause.className = "";
		playpause.className = "play";
	}
}