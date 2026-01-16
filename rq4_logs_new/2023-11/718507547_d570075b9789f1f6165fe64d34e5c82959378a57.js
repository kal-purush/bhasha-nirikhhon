console.log("SPotifynnnjjjjjjjjjn");


let songIndex = 0;
let audioElement = new Audio('song/7.mp3');
let masterPlay = document.getElementById('masterPlay');
let myProgressBar = document.getElementById('myProgressBar');
let gif = document.getElementById('gif');
let songItems =Array.from(document.getElementsByClassName("songItem"));



let Songs = [
     {songName: "Waaa", filePath: "song/2.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/3.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/4.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/5.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/6.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/7.mp3", coverPath: "covers/2.jpg"},
     {songName: "Waaa", filePath: "song/8.mp3", coverPath: "covers/2.jpg"},

]






masterPlay.addEventListener('click', ()=>{
             if(audioElement.paused || audioElement.currentTime<=0){
              audioElement.play(); 
              masterPlay.classList.remove('fa-circle-play');
              masterPlay.classList.add('fa-circle-pause');
              gif.style.opacity = 1;
             }  
             else{
                audioElement.pause(); 
              masterPlay.classList.remove('fa-circle-pause');
              masterPlay.classList.add('fa-circle-play');
              gif.style.opacity = 0;
             }
})

audioElement.addEventListener('timeupdate',()=>{
    console.log('timeupdate');
    //update seekbar
    progress = parseInt((audioElement.currentTime/audioElement.duration)* 100);
    console.log(progress);
    myProgressBar.value = progress;
})


myProgressBar.addEventListener('input', () => {
  audioElement.currentTime = myProgressBar.value * audioElement.duration / 100;
});


const makeAllPlays = ()=>{
  Array.from(document.getElementsByClassName("songItemPlay")).forEach((element)=>{
  element.classList.remove('fa-circle-pause');
    element.classList.add('fa-circle-play');
  
  })
  }

  Array.from(document.getElementsByClassName("songItemPlay")).forEach((element) =>{
    element.addEventListener('click', (e) => {
      // Remove the 'fa-circle-play' class and add the 'fa-circle-pause' class
     makeAllPlays();
     index = parseInt(e.target.id);
      e.target.classList.remove('fa-circle-play');
      e.target.classList.add('fa-circle-pause');
         
      audioElement.src = "song/7.mp3";
      audioElement.currentTime = 0;
      audioElement.play();
  
      // You may want to pause the audio or handle your play/pause logic here
      // For simplicity, I'm assuming you have an `audioElement` variable
      // and toggling its playback state like this:
      if (audioElement.paused) {
        audioElement.play();
      } else {
        audioElement.pause();
      }
    });
  });
  
