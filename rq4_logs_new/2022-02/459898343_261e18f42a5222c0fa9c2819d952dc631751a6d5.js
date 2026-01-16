// Select all the elements in the HTML page
// and assign them to a variable
let dropdown = document.getElementById(`drop`);
let now_playing = document.querySelector(".now-playing");
let track_art = document.querySelector(".track-art");
let track_name = document.querySelector(".track-name");
let track_artist = document.querySelector(".track-artist");

let playpause_btn = document.querySelector(".playpause-track");
let next_btn = document.querySelector(".next-track");
let prev_btn = document.querySelector(".prev-track");

let seek_slider = document.querySelector(".seek_slider");
let volume_slider = document.querySelector(".volume_slider");
let curr_time = document.querySelector(".current-time");
let total_duration = document.querySelector(".total-duration");

// Specify globally used values
let track_index = 0;
let isPlaying = false;
let updateTimer;
let songa = document.getElementById(`1s`);
// Create the audio element for the player
let curr_track = document.createElement('audio');

// Define the list of tracks that have to be played
let track_list = [
{
	name: "The Nights",
	artist: "Avicci",
	image: "images/1.png",
	path: "songs/1.mp3"
},
{
	name: "Alone",
	artist: "Marshmello",
	image: "images/2.jpg",
	path: "songs/2.mp3"
},
{
	name: "Something Just Like This",
	artist: "Alesso Remix",
	image: "images/3.jpg",
	path: "songs/3.mp3",
},
{
	name: "High On Life",
	artist: "Bonn",
	image: "images/4.jpg",
	path: "songs/4.mp3",
},
{
	name: "The Spectre",
	artist: "Alan Walker",
	image: "images/5.jpg",
	path: "songs/5.mp3",
},
{
	name: "No Promises",
	artist: "ft. Demi Lovato",
	image: "images/6.jpg",
	path: "songs/6.mp3",
},
{
	name: "Wildest Dreams",
	artist: "Thomas Gold",
	image: "images/7.jpg",
	path: "songs/7.mp3",
},
{
	name: "Better When You're Gone",
	artist: "David Guetta",
	image: "images/8.jpg",
	path: "songs/8.mp3",
},
{
	name: "Move Your Body ",
	artist: "Sia",
	image: "images/9.jpg",
	path: "songs/9.mp3",
},
{
	name: "Wake Me Up",
	artist: "Avicci",
	image: "images/2.jpg",
	path: "songs/10.mp3",
},
{
	name: "Heaven",
	artist: "Dzeko",
	image: "images/9.jpg",
	path: "songs/11.mp3",
},
{
	name: "Ritual",
	artist: "Rita Ora",
	image: "images/9.jpg",
	path: "songs/12.mp3",
},
{
	name: "Heros (We Could Be)",
	artist: "Alesso",
	image: "images/2.jpg",
	path: "songs/13.mp3",
},
{
	name: "Lullaby",
	artist: "R3HAB",
	image: "images/2.jpg",
	path: "songs/14.mp3",
},
{
	name: "I'm Not Sorry",
	artist: "Hardwell",
	image: "images/2.jpg",
	path: "songs/15.mp3",
},
{
	name: "Sweet But Psyho",
	artist: "Ava Max",
	image: "images/3.jpg",
	path: "songs/16.mp3",
},
{
	name: "Move Your Body ",
	artist: "Sia",
	image: "images/2.jpg",
	path: "songs/17.mp3",
},
{
	name: "If I lose Myself",
	artist: "OneRepublic",
	image: "images/4.jpg",
	path: "songs/18.mp3",
},
{
	name: "New- Ryan Riback",
	artist: "Daya",
	image: "images/2.jpg",
	path: "songs/19.mp3",
},
{
	name: "Carry You Home",
	artist: "Tiesto",
	image: "images/2.jpg",
	path: "songs/20.mp3",
},
{
	name: "Sweet Escape",
	artist: "Alesso",
	image: "images/2.jpg",
	path: "songs/21.mp3",
},
{
	name: "Only Want You",
	artist: "Rita Ora",
	image: "images/4.jpg",
	path: "songs/22.mp3",
},
{
	name: "Body Back",
	artist: "Gryffin",
	image: "images/2.jpg",
	path: "songs/23.mp3",
},
{
	name: "Like I DO",
	artist: "David Guetta",
	image: "images/2.jpg",
	path: "songs/24.mp3",
},
{
	name: "Faded Remix",
	artist: "Northen Lights Remix",
	image: "images/2.jpg",
	path: "songs/25.mp3",
},
{
	name: "Mama",
	artist: "Clean Bandit",
	image: "images/2.jpg",
	path: "songs/26.mp3",
},
{
	name: "Sick Boy ",
	artist: "Chainsmokers",
	image: "images/2.jpg",
	path: "songs/27.mp3",
},
{
	name: "Silence",
	artist: "Marshmello",
	image: "images/2.jpg",
	path: "songs/28.mp3",
},
{
	name: "SOS",
	artist: "Tribute Remix",
	image: "images/2.jpg",
	path: "songs/29.mp3",
},
{
	name: "YourSOng",
	artist: "Ritaa",
	image: "images/2.jpg",
	path: "songs/30.mp3",
},
{
	name: "DOnt YOu Worry Child",
	artist: "Radio Edit",
	image: "images/2.jpg",
	path: "songs/31.mp3",
},
{
	name: "Solo",
	artist: "Lovato",
	image: "images/2.jpg",
	path: "songs/32.mp3",
},
{
	name: "Power",
	artist: "Hard well",
	image: "images/2.jpg",
	path: "songs/33.mp3",
},
{
	name: "Sweet Dreams",
	artist: "Alan Walker",
	image: "images/2.jpg",
	path: "songs/34.mp3",
},
{
	name: "I'm Glad You came",
	artist: "The Wanted",
	image: "images/2.jpg",
	path: "songs/35.mp3",
},
{
	name: "Runaway (U&I)",
	artist: "Galantis",
	image: "images/2.jpg",
	path: "songs/36.mp3",
},
{
	name: "Chemials",
	artist: "Don Diablo",
	image: "images/2.jpg",
	path: "songs/37.mp3",
},
{
	name: "Let Me Love You ",
	artist: "Don Diablo",
	image: "images/2.jpg",
	path: "songs/38.mp3",
},
{
	name: "In My Mind",
	artist: "Dynoro",
	image: "images/2.jpg",
	path: "songs/39.mp3",
},
{
	name: "Around The World La La La",
	artist: "ATC",
	image: "images/2.jpg",
	path: "songs/40.mp3",
},
{
	name: "Chain My Heart",
	artist: "Topic",
	image: "images/2.jpg",
	path: "songs/41.mp3",
},
{
	name: "Summer Days",
	artist: "Martin",
	image: "images/2.jpg",
	path: "songs/42.mp3",
},
{
	name: "Ritual",
	artist: "Marshmello",
	image: "images/2.jpg",
	path: "songs/43.mp3",
},
{
	name: "Don't You Hold Me Down",
	artist: "Alan Walker",
	image: "images/2.jpg",
	path: "songs/44.mp3",
},
{
	name: "Solo",
	artist: "Faustic",
	image: "images/2.jpg",
	path: "songs/45.mp3",
},
{
	name: "Louder",
	artist: "Radio Edit",
	image: "images/2.jpg",
	path: "songs/46.mp3",
},
{
	name: "AstroNaut In The Ocean",
	artist: "Crystal Rock",
	image: "images/2.jpg",
	path: "songs/47.mp3",
},
{
	name: "Gaulin ",
	artist: "Moon Light",
	image: "images/2.jpg",
	path: "songs/48.mp3",
},
{
	name: "Impossible ",
	artist: "John Martin",
	image: "images/2.jpg",
	path: "songs/49.mp3",
},
{
	name: "Boyfriend",
	artist: "Mabel",
	image: "images/2.jpg",
	path: "songs/50.mp3",
},
{
	name: "Cool Kids",
	artist: "Olympis",
	image: "images/2.jpg",
	path: "songs/51.mp3",
},
{
	name: "On & On",
	artist: "Alok",
	image: "images/2.jpg",
	path: "songs/52.mp3",
},
{
	name: "Blah Blah Blah ",
	artist: "Armin",
	image: "images/2.jpg",
	path: "songs/53.mp3",
},
{
	name: "Another YOu",
	artist: "Armin",
	image: "images/2.jpg",
	path: "songs/54.mp3",
},
{
	name: "Five More Hours",
	artist: "Deorro",
	image: "images/2.jpg",
	path: "songs/55.mp3",
},
{
	name: "Magenta Riddim",
	artist: "Dj Snake",
	image: "images/2.jpg",
	path: "songs/56.mp3",
},
{
	name: "I'm Blue",
	artist: "Club Mix",
	image: "images/2.jpg",
	path: "songs/57.mp3",
},
{
	name: "Bang Bang",
	artist: "Jessie",
	image: "images/2.jpg",
	path: "songs/58.mp3",
},
{
	name: "OUT OUT",
	artist: "Joel",
	image: "images/2.jpg",
	path: "songs/59.mp3",
},
{
	name: "Freak On leash",
	artist: "Korn",
	image: "images/2.jpg",
	path: "songs/60.mp3",
},
{
	name: "Summer Feeling",
	artist: "Matoma",
	image: "images/2.jpg",
	path: "songs/61.mp3",
},
{
	name: "Back & Forth",
	artist: "Jonas Blue",
	image: "images/2.jpg",
	path: "songs/62.mp3",
},
{
	name: "Pure Grinding",
	artist: "ISHI Remix",
	image: "images/2.jpg",
	path: "songs/63.mp3",
},
{
	name: "Say It Right",
	artist: "Iluro Remix",
	image: "images/2.jpg",
	path: "songs/64.mp3",
},
{
	name: "Azukita",
	artist: "Steve",
	image: "images/2.jpg",
	path: "songs/65.mp3",
},
{
	name: "BOOM",
	artist: "Tiesto",
	image: "images/2.jpg",
	path: "songs/66.mp3",
},
{
	name: "Drop That Low",
	artist: "Tujamo",
	image: "images/2.jpg",
	path: "songs/67.mp3",
},
{
	name: "Rise Up",
	artist: "VINAI",
	image: "images/2.jpg",
	path: "songs/68.mp3",
},
{
	name: "Party Till We Die",
	artist: "MAKJ",
	image: "images/2.jpg",
	path: "songs/69.mp3",
},
{
	name: "New Rules",
	artist: "KREAM REMIX",
	image: "images/2.jpg",
	path: "songs/70.mp3",
},
{
	name: "W&W",
	artist: "The One",
	image: "images/2.jpg",
	path: "songs/71.mp3",
},
];



function loadTrack(track_index) {
    // Clear the previous seek timer
    clearInterval(updateTimer);
    resetValues();
    
    // Load a new track
    curr_track.src = track_list[track_index].path;
    curr_track.load();
    
    // Update details of the track
    track_art.style.backgroundImage =
        "url(" + track_list[track_index].image + ")";
    track_name.textContent = track_list[track_index].name;
    track_artist.textContent = track_list[track_index].artist;
    now_playing.textContent =
        "PLAYING " + (track_index + 1) + " OF " + track_list.length;
    
    // Set an interval of 1000 milliseconds
    // for updating the seek slider
    updateTimer = setInterval(seekUpdate, 1000);
    
    // Move to the next track if the current finishes playing
    // using the 'ended' event
    curr_track.addEventListener("ended", nextTrack);
    
    // Apply a random background color
    random_bg_color();
    }
    
    // function random_bg_color() {
    // // Get a random number between 64 to 256
    // // (for getting lighter colors)
    // let red = Math.floor(Math.random() * 256) + 64;
    // let green = Math.floor(Math.random() * 256) + 64;
    // let blue = Math.floor(Math.random() * 256) + 64;
    
    // // Construct a color withe the given values
    // let bgColor = "rgb(" + red + ", " + green + ", " + blue + ")";
    
    // // Set the background to the new color
    // document.body.style.background = bgColor;
    // }
    
    // Function to reset all values to their default
    function resetValues() {
    curr_time.textContent = "00:00";
    total_duration.textContent = "00:00";
    seek_slider.value = 0;
    }
    shuffle(track_list);
 function shuffle(){
    let array= track_list;
     let cuIndex = array.length, temporaryValue, randomIndex ;
     // While there remain elements to shuffle...
     while (0 !== cuIndex) {
       // Pick a remaining element...
       randomIndex = Math.floor(Math.random() * cuIndex);
       cuIndex -= 1;
  
       // And swap it with the current element.
       temporaryValue = array[cuIndex];
       array[cuIndex] = array[randomIndex];
       array[randomIndex] = temporaryValue;
    
  
     return array;
 }
 
 }

    function playpauseTrack() {
        // Switch between playing and pausing
        // depending on the current state
        if (!isPlaying) playTrack();
        else pauseTrack();
        }
        
        function playTrack() {
        // Play the loaded track
        curr_track.play();
        isPlaying = true;
        
        // Replace icon with the pause icon
        playpause_btn.innerHTML = '<i class="fa fa-pause-circle fa-5x"></i>';
        }
        
        function pauseTrack() {
        // Pause the loaded track
        curr_track.pause();
        isPlaying = false;
        
        // Replace icon with the play icon
        playpause_btn.innerHTML = '<i class="fa fa-play-circle fa-5x"></i>';
        }
        
        function nextTrack() {
        // Go back to the first track if the
        // current one is the last in the track list
        if (track_index < track_list.length - 1)
            track_index += 1;
        else track_index = 0;
        
        // Load and play the new track
        loadTrack(track_index);
        playTrack();
        }
		
        function prevTrack() {
        // Go back to the last track if the
        // current one is the first in the track list
        if (track_index > 0)
            track_index -= 1;
        else track_index = track_list.length - 1;
            
        // Load and play the new track
        loadTrack(track_index);
        playTrack();
        }
        function seekTo() {
            // Calculate the seek position by the
            // percentage of the seek slider
            // and get the relative duration to the track
            seekto = curr_track.duration * (seek_slider.value / 100);
            
            // Set the current track position to the calculated seek position
            curr_track.currentTime = seekto;
            }
            
            function setVolume() {
            // Set the volume according to the
            // percentage of the volume slider set
            curr_track.volume = volume_slider.value / 100;
            }
            
            function seekUpdate() {
            let seekPosition = 0;
            
            // Check if the current track duration is a legible number
            if (!isNaN(curr_track.duration)) {
                seekPosition = curr_track.currentTime * (100 / curr_track.duration);
                seek_slider.value = seekPosition;
            
                // Calculate the time left and the total duration
                let currentMinutes = Math.floor(curr_track.currentTime / 60);
                let currentSeconds = Math.floor(curr_track.currentTime - currentMinutes * 60);
                let durationMinutes = Math.floor(curr_track.duration / 60);
                let durationSeconds = Math.floor(curr_track.duration - durationMinutes * 60);
            
                // Add a zero to the single digit time values
                if (currentSeconds < 10) { currentSeconds = "0" + currentSeconds; }
                if (durationSeconds < 10) { durationSeconds = "0" + durationSeconds; }
                if (currentMinutes < 10) { currentMinutes = "0" + currentMinutes; }
                if (durationMinutes < 10) { durationMinutes = "0" + durationMinutes; }
            
                // Display the updated duration
                curr_time.textContent = currentMinutes + ":" + currentSeconds;
                total_duration.textContent = durationMinutes + ":" + durationSeconds;
            }
            }
// Load the first track in the tracklist
loadTrack(track_index);
let then =document.getElementById(`then`);
        songa.addEventListener('click', ()=>{
			if(then.paused || then.currentTime<=0){
				then.play();
				songa.classList.remove('fa-circle-play')
				songa.classList.add('fa-circle-pause')
			     masterPlay.classList.remove('fa-circle-play')
			     masterPlay.classList.add('fa-circle-pause')
			}
			else{
				then.pause();
				songa.classList.remove('fa-circle-pause')
				songa.classList.add('fa-circle-play')
			     playpause_btn.classList.remove('fa-circle-pause')
			     masterPlay.classList.add('fa-circle-play')
			}
		})