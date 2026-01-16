function toggleNya() {
    var nyaudio = document.getElementById('nyaaaaduio');
    var nyacon = document.getElementById('nyacon')
    if(nyaudio.paused) {
        nyaudio.play();
    }
    if(nyaudio.muted) {
        nyaudio.muted = false;
        nyacon.innerText = "volume_off";
    } else {
        nyaudio.muted = true;
        nyacon.innerText = "volume_up";
    }
}