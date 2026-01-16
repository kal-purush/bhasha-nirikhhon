console.log("Welcome to music maniac");

let songIndex = 0;
let audioElement = new Audio('1.mp3');
let masterPlay = document.getElementById('masterPlay');
let myProgressBar = document.getElementById('myProgressBar');
let gif = document.getElementById('gif');
let songItems = Array.from(document.getElementsByClassName('songItem'));
let masterSongName = document.getElementById('masterSongName');


let songs= [
    {songName: "Main Agar Kahoon", filePath: "1.mp3", coverPath: "cover1.jpg"},
    {songName: "Ankho Mein Teri", filePath: "2.mp3", coverPath: "cover2.jpg"},
    {songName: "Ham Mar Jayenge", filePath: "3.mp3", coverPath: "cover3.jpg"},
    {songName: "Chand Sifaris", filePath: "4.mp3", coverPath: "cover6.jpg"},
    {songName: "Majhe Majhe Tobo Dekha Pai", filePath: "5.mp3", coverPath: "cover4.jpg"},
    {songName: "Amaro Porano Jaha Chai", filePath: "6.mp3", coverPath: "cover5.jpg"},
    {songName: "Tomake Chai", filePath: "7.mp3", coverPath: "cover5.jpg"},
    {songName: "Main Agar Kahoon 3D", filePath: "8.mp3", coverPath: "cover1.jpg"},
    {songName: "Ankho Mein Teri 3D", filePath: "9.mp3", coverPath: "cover2.jpg"},
    {songName: "Ham Mar Jayenge 3D", filePath: "10.mp3", coverPath: "cover3.jpg"},
    {songName: "Chand Sifaris 3D", filePath: "11.mp3", coverPath: "cover6.jpg"},
    {songName: "Majhe Majhe Tobo Dekha Pai 3D", filePath: "12.mp3", coverPath: "cover4.jpg"},
    {songName: "Amaro Porano Jaha Chai 3D", filePath: "13.mp3", coverPath: "cover5.jpg"},
    {songName: "Tomake Chai 3D", filePath: "14.mp3", coverPath: "cover5.jpg"},
]

songItems.forEach((element, i)=>{ 
    element.getElementsByTagName("img")[0].src = songs[i].coverPath; 
    element.getElementsByClassName("songName")[0].innerText = songs[i].songName; 
})



// Handle play/pause click
masterPlay.addEventListener('click', ()=>{
    if(audioElement.paused || audioElement.currentTime<=0){
        audioElement.play();
        masterPlay.classList.remove('fa-play-circle');
        masterPlay.classList.add('fa-pause-circle');
        gif.style.opacity = 1;
    }
    else{
        audioElement.pause();
        masterPlay.classList.remove('fa-pause-circle');
        masterPlay.classList.add('fa-play-circle');
        gif.style.opacity = 0;
    }
})

// Listen to Events
audioElement.addEventListener('timeupdate', ()=>{ 
    // Update Seekbar
    progress = parseInt((audioElement.currentTime/audioElement.duration)* 100); 
    myProgressBar.value = progress;
})

myProgressBar.addEventListener('change', ()=>{
    audioElement.currentTime = myProgressBar.value * audioElement.duration/100;
})

const makeAllPlays = ()=>{
    Array.from(document.getElementsByClassName('songItemPlay')).forEach((element)=>{
        element.classList.remove('fa-pause-circle');
        element.classList.add('fa-play-circle');
    })
}

 

Array.from(document.getElementsByClassName('songItemPlay')).forEach((element)=>{
    element.addEventListener('click', (e)=>{ 
        makeAllPlays();
        songIndex = parseInt(e.target.id);
        e.target.classList.remove('fa-play-circle');
        e.target.classList.add('fa-pause-circle');
        audioElement.src = `${songIndex+1}.mp3`;
        masterSongName.innerText = songs[songIndex].songName;
        
        audioElement.currentTime = 0;
        audioElement.play();
        gif.style.opacity = 1;
        masterPlay.classList.remove('fa-play-circle');
        masterPlay.classList.add('fa-pause-circle');
    })
})

Array.from(document.getElementsByClassName('songItem3dPlay')).forEach((element)=>{
    
    element.addEventListener('click', (e)=>{ 
        makeAllPlays();
        songIndex = parseInt(e.target.id);
        e.target.classList.remove('fa-play-circle');
        e.target.classList.add('fa-pause-circle');
        audioElement.src = `${songIndex+1}.mp3`;
        masterSongName.innerText = songs[songIndex].songName;
        audioElement.currentTime = 0;
        audioElement.play();
        gif.style.opacity = 1;
        masterPlay.classList.remove('fa-play-circle');
        masterPlay.classList.add('fa-pause-circle');
    })
})


document.getElementById('next').addEventListener('click', ()=>{
    if(songIndex>=13){
        songIndex = 0
    }
    else{
        songIndex += 1;
    }
    audioElement.src = `${songIndex+1}.mp3`;
    masterSongName.innerText = songs[songIndex].songName;
    audioElement.currentTime = 0;
    audioElement.play();
    masterPlay.classList.remove('fa-play-circle');
    masterPlay.classList.add('fa-pause-circle');

})

document.getElementById('previous').addEventListener('click', ()=>{
    if(songIndex<=0){
        songIndex = 0
    }
    else{
        songIndex -= 1;
    }
    audioElement.src = `${songIndex+1}.mp3`;
    masterSongName.innerText = songs[songIndex].songName;
    audioElement.currentTime = 0;
    audioElement.play();
    masterPlay.classList.remove('fa-play-circle');
    masterPlay.classList.add('fa-pause-circle');
})