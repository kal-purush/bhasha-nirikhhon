const songs = [
    "Turbo - Soundtrack - 11 - The Snail is Fast.mp3",
    "Money.mp3",
    "Kiss and Make Up.mp3",
    "Grag Queen - ESTILO ANA MARIA.mp3",
    "Enemy (From the series Arcane League of Legends).mp3",
    "Starting Now - Brandy (Audio).mp3",
  ];
  const player = document.getElementById("player");
  
  const createSongList = () => {
    const list = document.createElement("ol");
    for (let i = 0; i < songs.length; i++) {
      const item = document.createElement("li");
      item.appendChild(document.createTextNode(songs[i]));
      list.appendChild(item);
    }
    return list;
  };
  
  const songList = document.getElementById("songList");
  songList.appendChild(createSongList());
  const links = document.querySelectorAll("li");
  for (const link of links) {
    link.addEventListener("click", setSong);
  }
  
  function setSong(e) {
    document.querySelector("#headphones").classList.remove("pulse");
  
    const source = document.getElementById("source");
    source.src = "songs/" + e.target.innerText;
    document.getElementById(
      "currentSong"
    ).innerText = `Now Playing:  ${e.target.innerText}`;
  
    player.load();
    player.play();
  
    document.querySelector("#headphones").classList.add("pulse");
  }
  
  function playAudio() {
    if (player.readyState) {
      player.play();
    }
  }
  
  function pauseAudio() {
    player.pause();
  }
  
  const slider = document.getElementById("volumeSlider");
  slider.oninput = function (e) {
    const volume = e.target.value;
    player.volume = volume;
  };
  
  function updateProgress() {
    if (player.currentTime > 0) {
      const progressBar = document.getElementById("progress");
      progressBar.value = (player.currentTime / player.duration) * 100;
    }
  }

// update progress bar width according to music current time
mainAudio.addEventListener("timeupdate", (e)=>{
    const currentTime = e.target.currentTime; //getting playing song currentTime
    const duration = e.target.duration; //getting playing song total duration
    let progressWidth = (currentTime / duration) * 100;
    progressBar.style.width = `${progressWidth}%`;
    let musicCurrentTime = wrapper.querySelector(".current-time"),
    musicDuartion = wrapper.querySelector(".max-duration");
    mainAudio.addEventListener("loadeddata", ()=>{
      // update song total duration
      let mainAdDuration = mainAudio.duration;
      let totalMin = Math.floor(mainAdDuration / 60);
      let totalSec = Math.floor(mainAdDuration % 60);
      if(totalSec < 10){ //if sec is less than 10 then add 0 before it
        totalSec = `0${totalSec}`;
      }
      musicDuartion.innerText = `${totalMin}:${totalSec}`;
    });
    // update playing song current time
    let currentMin = Math.floor(currentTime / 60);
    let currentSec = Math.floor(currentTime % 60);
    if(currentSec < 10){ //if sec is less than 10 then add 0 before it
      currentSec = `0${currentSec}`;
    }
    musicCurrentTime.innerText = `${currentMin}:${currentSec}`;
  });
  // update playing song currentTime on according to the progress bar width
  progressArea.addEventListener("click", (e)=>{
    let progressWidth = progressArea.clientWidth; //getting width of progress bar
    let clickedOffsetX = e.offsetX; //getting offset x value
    let songDuration = mainAudio.duration; //getting song total duration
    
    mainAudio.currentTime = (clickedOffsetX / progressWidth) * songDuration;
    playMusic(); //calling playMusic function
    playingSong();
  });
  