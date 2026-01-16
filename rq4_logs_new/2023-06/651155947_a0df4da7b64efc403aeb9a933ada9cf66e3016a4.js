var mcaudioPlayer = document.querySelector('.mc-audioPlayer');

var mcPlay = document.querySelector('.mc-play');
var mcPause = document.querySelector('.mc-pause');
var mcStop = document.querySelector('.stop');

var songStart = document.querySelector('.song-start');
var songEnd = document.querySelector('.song-end');
var forwardBtn = document.querySelector('.forward');
var backBtn = document.querySelector('.back');

var songRange = document.querySelector('#song-range');


var handle = function (e) {
    if (e.target == mcPlay) {
        mcPlay.classList.add('hide');
        mcPause.classList.remove('hide');
        mcaudioPlayer.play();
    }
    if (e.target == mcPause) {
        mcPause.classList.add('hide');
        mcPlay.classList.remove('hide');
        mcaudioPlayer.pause();
    }
    if (e.target == forwardBtn && (!mcaudioPlayer.paused)) {
        mcaudioPlayer.currentTime += 10;
    }
    if (e.target == backBtn && (!mcaudioPlayer.paused)) {
        mcaudioPlayer.currentTime -= 10;
    }
    if (e.target == mcStop) {
        mcPause.classList.add('hide');
        mcPlay.classList.remove('hide');
        mcaudioPlayer.pause();
        mcaudioPlayer.currentTime = 0;
    }

}

document.addEventListener('click', handle);

mcaudioPlayer.addEventListener('loadedmetadata', function () {
    var totalTime = mcaudioPlayer.duration; // Get the total time in seconds
    var minutes = Math.floor(totalTime / 60); // Get the minutes
    if (minutes < 10) {
        minutes = "0" + minutes;
    }
    var seconds = Math.floor(totalTime % 60); // Get the seconds
    var endTime = minutes + ":" + seconds;
    songEnd.innerText = endTime;
});

mcaudioPlayer.addEventListener('timeupdate', () => {
    var minutes = Math.floor(mcaudioPlayer.currentTime / 60); // Get the minutes
    var seconds = Math.floor(mcaudioPlayer.currentTime % 60); // Get the seconds
    if (seconds < 10) {
        seconds = "0" + seconds;
    }
    if (minutes < 10) {
        minutes = "0" + minutes;
    }
    songStart.innerText = minutes + ":" + seconds;
});

mcaudioPlayer.addEventListener('timeupdate', () => {
    songRange.value = mcaudioPlayer.currentTime;
});

songRange.addEventListener('input', () => {
    mcaudioPlayer.currentTime = songRange.value;
});
