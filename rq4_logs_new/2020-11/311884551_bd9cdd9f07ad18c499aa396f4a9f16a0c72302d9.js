// Ctrl buttons 
const playBtn = document.querySelector('#play'),
 previous = document.querySelector('#prev'),
 next = document.querySelector('#next'),
 music = document.querySelector('audio');

 // Container
 const progressContainer = document.querySelector('.progress-container');
 const progress = document.querySelector('.progress');

// Song, Artist and images
const img = document.querySelector('img');
const songTitle = document.querySelector('#title');
const singer = document.querySelector('#artist');

// Timing
const currentDuration = document.querySelector('#current-time');
const durationEl = document.querySelector('#duration');

// Volume
const volumeUp = document.querySelector('.increase');
const muteBtn = document.querySelector('.decrease');

// Song collection
const songs = [
    {
        name: 'nepali-1',
        displayName : 'Abhiman',
        artist: 'Albatross'

    },

    {
        name: 'nepali-3',
        displayName : 'Hatkela',
        artist: 'Jindabaad'
    },

    {
        name: 'komal',
        displayName : 'Komal Tyo Timro',
        artist: 'Sabin Rai'
    },
    {
        name: 'weeknd',
        displayName : 'I feel it Coming',
        artist: 'The Weeknd'
    },
    
    {
        name: 'nepali-2',
        displayName : 'Bachau',
        artist: 'Albatross'
    }
]

// Set boolean val to initial play
 let isPlaying = false;
 
 function playSong(){
     isPlaying = true;
     music.play();
     playBtn.classList.replace('fa-play-circle', 'fa-pause-circle')
     playBtn.setAttribute('title', 'Pause');
 }

 function pauseSong(){
     isPlaying = false;
     music.pause();
     playBtn.classList.replace('fa-pause-circle', 'fa-play-circle');
     playBtn.setAttribute('title', 'Play');
 }

 function controlMusicPlayer(){
    isPlaying ? pauseSong() : playSong();
 }

function loadSong(song){
    img.src = `img/${song.name}.jpg`;
    music.src = `music/${song.name}.mp3`;
    songTitle.textContent = song.displayName;
    singer.textContent = song.artist;
}
// Current song
let songIndex = 0;

// Previous song
function previousSong(){
    songIndex--;
    if(songIndex < 0){
        songIndex = songs.length - 1;
    }
    loadSong(songs[songIndex]);
    playSong()
}

// Next song
function nextSong(){
    songIndex++;
    if(songIndex > (songs.length - 1)){
        songIndex = 0;
    }
    loadSong(songs[songIndex]);
    playSong()
}

// On load- select first song
loadSong(songs[songIndex]);

// Update progress bar & time
function updateProgressBar(e){
      if(isPlaying){ 
      const {currentTime, duration} = e.srcElement;

      // Update progress bar width
      const progressPercent = (currentTime / duration) * 100
      progress.style.width = `${progressPercent}%`;

      //Calc duration minute
        const durationMinute = Math.floor(duration / 60);
        let durationSec = Math.floor((duration % 60));
        if(durationSec < 10){
            durationSec = `0${durationSec}`;
        }
      if(durationSec){ 
        document.getElementById('duration').textContent = `${durationMinute}:${durationSec}`;
    }

    // Calc current time
    const currentMinute = Math.floor(currentTime / 60);
    let currentSec = Math.floor((currentTime % 60));
    if(currentSec < 10){
        currentSec = `0${currentSec}`;
    }
    currentDuration.textContent = `${currentMinute}:${currentSec}`;
  }
}

function setProgressBar(e){
    const width = e.srcElement.clientWidth;
    const clickedWidth = e.offsetX;
    console.log(width, clickedWidth);
    const {duration} = music;
    music.currentTime = clickedWidth/width * duration;
    
}

function changeVolume(){
    music.volume = 0;
    muteBtn.classList.remove('mute');
    volumeUp.classList.add('mute');
}

function muteVolume(){
    music.volume = 1;
    muteBtn.classList.add('mute');
    volumeUp.classList.remove('mute');
}

 // Event Listeners
playBtn.addEventListener('click', controlMusicPlayer);
document.addEventListener('keypress', e => {
    if(e.keyCode === 13 || e.keyCode === 32){
        controlMusicPlayer();
    }
})
previous.addEventListener('click', previousSong);
next.addEventListener('click', nextSong);
music.addEventListener('timeupdate', updateProgressBar);
music.addEventListener('ended', nextSong);
progressContainer.addEventListener('click', setProgressBar);
volumeUp.addEventListener('click', changeVolume);
muteBtn.addEventListener('click', muteVolume);






