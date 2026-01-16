console.log("script loadded successfully")
var audio = new Audio('sound.mp3');
window.addEventListener("DOMContentLoaded", event => {
    audio.volume = 0.2;
    audio.play()
    setInterval(function(){  audio.play();},1)
  });