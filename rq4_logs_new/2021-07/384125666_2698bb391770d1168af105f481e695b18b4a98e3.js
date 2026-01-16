const playerBody = document.querySelector(".player-body");
const coverIMG = document.querySelector(".cover-img");

const repeatButton = document.querySelector(".repeat");
const shuffleButton = document.querySelector(".shuffle");

const titleText = document.querySelector(".title");
const authorText = document.querySelector(".author");
const progressTimeText = document.querySelector(".progress-time");
const lengthText = document.querySelector(".length");
const progressBar = document.querySelector(".progressbar");

const playButton = document.querySelector(".play-background");
const prevButton = document.querySelector(".prev");
const nextButton = document.querySelector(".next");

var progressTime = 62;
var length = 139;

setProgressTime(progressTimeText, progressTime);
setProgressTime(lengthText, length);
setProgrressBar();

function setProgressTime(DOM, time) {
    DOM.innerText += Math.floor(time / 60) + ":";
    if (time % 60 < 10) DOM.innerText += "0";
    DOM.innerText += time % 60;
}

function setProgrressBar() {
    progressBar.style.width = `${(progressTime / length) * 100}%`;
}

function setBackground() {
    playerBody.style.background = `center / cover url(${coverIMG.src}) no-repeat`;
}

repeatButton.addEventListener("click", () => {
    repeatButton.classList.toggle("active");
});

shuffleButton.addEventListener("click", () => {
    shuffleButton.classList.toggle("active");
});

playButton.addEventListener("click", () => {
    const btn =
        document.querySelector(".play") || document.querySelector(".pause");
    if (btn.classList == "play") btn.classList = "pause";
    else btn.classList = "play";
});

let covers = ["BONES_HDMI", "LanaDelRey_BornToDie", "joji_demons"];
let titles = ["HDMI", "Born To Die", "demons"];
let authors = ["Bones", "Lana Del Rey", "joji"];
var currentSongIndex = 1;

coverIMG.src = `images/covers/${covers[currentSongIndex]}.jpg`;
titleText.innerText = titles[currentSongIndex];
authorText.innerText = authors[currentSongIndex];
setBackground();

prevButton.addEventListener("click", () => {
    if (!prevButton.classList.contains("active")) return;
    currentSongIndex--;
    changeSong();

    if (currentSongIndex == 0) prevButton.classList.remove("active");
    nextButton.classList.add("active");
});

nextButton.addEventListener("click", () => {
    if (!nextButton.classList.contains("active")) return;
    currentSongIndex++;
    changeSong();

    if (currentSongIndex == covers.length - 1)
        nextButton.classList.remove("active");
    prevButton.classList.add("active");
});

function changeSong() {
    coverIMG.src = `images/covers/${covers[currentSongIndex]}.jpg`;
    titleText.innerText = titles[currentSongIndex];
    authorText.innerText = authors[currentSongIndex];
    setBackground();
}