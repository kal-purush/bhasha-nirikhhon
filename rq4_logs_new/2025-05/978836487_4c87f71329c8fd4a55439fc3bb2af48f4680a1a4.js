console.log("Welcome to Spotify");

// Initialize variables
let songIndex = 0;
let audio = new Audio('songs/1.mp3');

let masterPlay = document.getElementById('masterPlay');
let myProgressBar = document.getElementById('my-progress-bar');
let gif = document.getElementById('gif');
let songItems = Array.from(document.getElementsByClassName('song-item'));
let masterSongName = document.getElementById('master-song-name');
let songItemPlays = Array.from(document.getElementsByClassName('song-item-play'));

let songs = [
    { songName: "Warriyo - Mortals [NCS Release]", filePath: "songs/1.mp3", coverPath: "covers/1.jpg" },
    { songName: "Cielo - Huma-Huma", filePath: "songs/2.mp3", coverPath: "covers/2.jpg" },
    { songName: "DEAF KEV - Invincible [NCS Release]-320k", filePath: "songs/3.mp3", coverPath: "covers/3.jpg" },
    { songName: "Different Heaven & EH!DE - My Heart [NCS Release]", filePath: "songs/4.mp3", coverPath: "covers/4.jpg" },
    { songName: "Janji-Heroes-Tonight-feat-Johnning-NCS-Release", filePath: "songs/5.mp3", coverPath: "covers/5.jpg" },
    { songName: "Samajavaragamana-Sid Sriram", filePath: "songs/6.mp3", coverPath: "covers/6.jpg" },
    { songName: "Bujji Thalli-Javed Ali", filePath: "songs/7.mp3", coverPath: "covers/7.jpg" },
    { songName: "Kissik-Sublashini", filePath: "songs/8.mp3", coverPath: "covers/8.jpg" },
    { songName: "Ninne Ninne-Armaan Malik", filePath: "songs/9.mp3", coverPath: "covers/7.jpg" },
]

songItems.forEach((element, i) => {
    element.getElementsByTagName('img')[0].src = songs[i].coverPath;
    element.getElementsByClassName("song-name")[0].innerText = songs[i].songName;
});

const updatePlayPauseIcons = () => {
    songItemPlays.forEach((element, i) => {
        if (i === songIndex) {
            if (audio.paused) {
                element.classList.remove('fa-pause-circle');
                element.classList.add('fa-play-circle');
            } else {
                element.classList.remove('fa-play-circle');
                element.classList.add('fa-pause-circle');
            }
        } else {
            element.classList.remove('fa-pause-circle');
            element.classList.add('fa-play-circle');
        }
    });
}


// Handle play/pause click on master play
masterPlay.addEventListener("click", () => {
    if (audio.paused || audio.currentTime <= 0) {
        audio.play();
        masterPlay.classList.remove('fa-play-circle');
        masterPlay.classList.add('fa-pause-circle');
        gif.style.opacity = 1;
        updatePlayPauseIcons();
    } else {
        audio.pause();
        masterPlay.classList.remove('fa-pause-circle');
        masterPlay.classList.add('fa-play-circle');
        gif.style.opacity = 0;
        updatePlayPauseIcons();
    }
})

// Listening to events
audio.addEventListener('timeupdate', () => {
    // Update seekbar only if duration is available
    if (!isNaN(audio.duration)) {
        progress = parseInt((audio.currentTime / audio.duration) * 100);
        myProgressBar.value = progress;
    }
});

myProgressBar.addEventListener('change', () => {
    // Update audio current time only if duration is available
    if (!isNaN(audio.duration)) {
        audio.currentTime = (myProgressBar.value * audio.duration) / 100;
    }
})

const makeAllPlays = () => {
    Array.from(document.getElementsByClassName('song-item-play')).forEach((element) => {
        element.classList.remove("fa-pause-circle");
        element.classList.add('fa-play-circle');
    })
}

Array.from(document.getElementsByClassName('song-item-play')).forEach((element) => {
    element.addEventListener("click", (e) => {
        if (audio.paused || songIndex !== parseInt(e.target.id)) {
            songIndex = parseInt(e.target.id);
            makeAllPlays();
            e.target.classList.remove("fa-play-circle");
            e.target.classList.add("fa-pause-circle");
            audio.src = `songs/${songIndex + 1}.mp3`;
            masterSongName.innerHTML = songs[songIndex].songName;
            audio.currentTime = 0;
            audio.play();
            gif.style.opacity = 1;
            masterPlay.classList.remove('fa-play-circle');
            masterPlay.classList.add('fa-pause-circle');
            updatePlayPauseIcons();
        } else {
            audio.pause();
            e.target.classList.remove("fa-pause-circle");
            e.target.classList.add("fa-play-circle");
            gif.style.opacity = 0;
            masterPlay.classList.add('fa-play-circle');
            masterPlay.classList.remove('fa-pause-circle');
        }
    })
})

document.getElementById('next').addEventListener("click", () => {

    if (songIndex >= 8) {
        songIndex = 0;
    } else {
        songIndex += 1;
    }
    audio.src = `songs/${songIndex + 1}.mp3`;
    masterSongName.innerHTML = songs[songIndex].songName;
    audio.currentTime = 0;
    audio.play();
    gif.style.opacity = 1;
    masterPlay.classList.remove('fa-play-circle');
    masterPlay.classList.add('fa-pause-circle');
    updatePlayPauseIcons();
})

document.getElementById('previous').addEventListener('click', () => {
    if (songIndex <= 0) {
        songIndex = 8
    } else {
        songIndex -= 1;
    }
    audio.src = `songs/${songIndex + 1}.mp3`;
    masterSongName.innerText = songs[songIndex].songName;
    audio.currentTime = 0;
    audio.play();
    gif.style.opacity = 1;
    masterPlay.classList.remove('fa-play-circle');
    masterPlay.classList.add('fa-pause-circle');
    updatePlayPauseIcons();
})

audio.addEventListener('ended', () => {
    if (songIndex >= 8) {
        songIndex = 0;
    } else {
        songIndex += 1;
    }
    audio.src = `songs/${songIndex + 1}.mp3`;
    masterSongName.innerHTML = songs[songIndex].songName;
    audio.currentTime = 0;
    audio.play();
    gif.style.opacity = 1;
    masterPlay.classList.remove('fa-play-circle');
    masterPlay.classList.add('fa-pause-circle');
    updatePlayPauseIcons();
});