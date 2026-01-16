"use strict";

//Dom elements
const inputBar = document.querySelector('#userInput');
const resetButton = document.querySelector('#reset');
const outputBar = document.querySelector('#outputWords');
const score = document.querySelector('#score');
const countdown = document.querySelector('#timer');
const result = document.querySelector('#result');
//List of words
const words = [];
//Time variables
const startingSeconds = 60;
let time = startingSeconds * 100;
var interval;
//Booleans for the interval
let typingStarted = false;
let timerRunning = false;
let readyToStart = true;
//Words counter
let correctWords = 0;
//Characters taken by words in the output section
let charactersTaken = 0;

//Listening for Enter or Space clicks to remove words and update the counter
inputBar.addEventListener('keyup', function(e) {
    if (e.code === "Enter" || e.code === "Space"){
        if (e.code === "Enter"){
            e.target.value += " ";
        }
        if (e.target.value === outputBar.firstChild.textContent){
            ++correctWords;
            score.innerHTML = correctWords;
            //Color carousel
            if (correctWords <= 10){
                let red = 20 + correctWords * 23;
                let green = 33 - correctWords * 3;
                let blue = 61 - correctWords * 6;
                //Getting to red 235 90 50 from dark blue 20 33 61
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            }
            else if (correctWords <= 20){
                let red = 255 - 2 * correctWords;
                let green = 9 * correctWords;
                let blue = 5 * correctWords;
                //Getting to red-orange 235 90 50 from pure red 255 0 0
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else if (correctWords <= 30) {
                let red = 235;
                let green = 90 + (correctWords - 10) * 11;
                let blue = 50;
                //Getting yellow 235 200 50
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else if (correctWords <= 40) {
                let red = 235 - (correctWords - 20) * 5;
                let green = 205 + (correctWords - 20) * 5;
                let blue = 50;
                //Getting acid-green 180 255 50
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else if (correctWords <= 50) {
                let red = 180 - (correctWords - 30) * 18;
                let green = 250 - (correctWords - 30) * 4;
                let blue = 50;
                //Getting nice-green 0 210 50
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else if (correctWords <= 60) {
                let red = 0;
                let green = 210 - (correctWords - 40) * 3;
                let blue = 50 + (correctWords - 40) * 15;
                //Getting light-blue 0 180 200
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else if (correctWords <= 60) {
                let red = (correctWords - 50) * 10;
                let green = 180 - (correctWords - 50) * 18;
                let blue = 200;
                //Getting purple 100 0 200
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            } else {
                let red = 160 - correctWords;
                let green = 0;
                let blue = 255 - correctWords;
                //Getting to black
                score.style.color = "rgb(" + red + "," + green + "," + blue + ")";
            }
        }
        charactersTaken -= (outputBar.firstChild.length - 1);
        outputBar.firstChild.remove();
        e.target.value = "";
        displayWords();
    }
    if (time <= 0){
        e.target.value = "";
        score.innerHTML = correctWords;
    }
});
//Event listener to start the timer
inputBar.addEventListener('keydown', function() {
    //Starting up the timer
    if (!typingStarted && !timerRunning && readyToStart){
        typingStarted = true;
        timerRunning = true;
        readyToStart = false;
        interval = setInterval(updateCountdown, 10);
    }
});
//Reset button event listener
resetButton.addEventListener('click', function() {
    //Display words again and hide result
    outputBar.className = "visible";
    result.className = "invisible";
    //Resetting the booleans
    typingStarted = false;
    timerRunning = false;
    readyToStart = true;
    //Resetting the time
    clearInterval(interval);
    time = startingSeconds * 100;
    countdown.innerHTML = "60:00";
    //Reset the word count
    correctWords = 0;
    score.innerHTML = correctWords;
    score.style.color = "#14213d";
    //Empty the input bar
    inputBar.value = "";
});


//Get random number function
const getRandomInt = (max) => Math.floor(Math.random() * max);
//Add words to the output section in DOM
function displayWords() {
    //60 chars per line, 2 lines => 120 chars
    while (charactersTaken < 150) {
        let currentWord = words[getRandomInt(100)];
        outputBar.appendChild(document.createTextNode(currentWord + ' '));
        charactersTaken += currentWord.length;
    }
}
//Update time function
function updateCountdown(){
    if (typingStarted && timerRunning){
        const seconds = Math.floor(time/100);
        let centiseconds = time % 100;
    
        centiseconds = centiseconds < 10 ? '0' + centiseconds : centiseconds;
        countdown.innerHTML = `${seconds}:${centiseconds}`;
        time--;
        if (time < 0){
            displayResult();
            typingStarted = false;
            timerRunning = false;
            outputBar.className = "invisible";
        }
    }
}
//Setting up the result element and its display function
function displayResult() {
    let wps = correctWords/60;
    wps = wps.toFixed(2);
    //Display results and hide words
    result.innerHTML = `Your typing speed is ${correctWords} WPM or ${wps} WPS.`;
    result.className = "visible";
}
//Setting up words
function setWords()
{
    words[0] = "ability";
    words[1] = "about";
    words[2] = "above";
    words[3] = "absolute";
    words[4] = "adult";
    words[5] = "afraid";
    words[6] = "African";
    words[7] = "ahead";
    words[8] = "and";

    words[9] = "bathroom";
    words[10] = "battery";
    words[11] = "beautiful";
    words[12] = "because";
    words[13] = "below";
    words[14] = "better";
    words[15] = "border";
    words[16] = "breakfast";
    words[17] = "brush";
    words[18] = "buy";
    words[19] = "break";

    words[20] = "cable";
    words[21] = "call";
    words[22] = "camera";
    words[23] = "center";
    words[24] = "chance";
    words[25] = "change";
    words[26] = "charge";
    words[27] = "clean";
    words[28] = "classroom";
    words[29] = "classic";
    words[30] = "cloud";
    words[31] = "coffee";
    words[32] = "cost";
    words[33] = "crew";
    words[34] = "currently";

    words[35] = "damage";
    words[36] = "dangerous";
    words[37] = "dark";
    words[38] = "data";
    words[39] = "dead";
    words[40] = "deliver";
    words[41] = "depth";
    words[42] = "dark";
    words[43] = "design";
    words[44] = "detail";
    words[45] = "development";
    words[46] = "device";
    words[47] = "dimension";
    words[48] = "dirt";
    words[49] = "discovery";
    words[50] = "discussion";
    words[51] = "document";
    words[52] = "dream";
    words[53] = "driver";

    words[54] = "earth";
    words[55] = "economics";
    words[56] = "educational";
    words[57] = "effective";
    words[58] = "efficiency";
    words[59] = "effort";
    words[60] = "emission";
    words[61] = "employee";
    words[62] = "empty";
    words[63] = "engineering";
    words[64] = "English";
    words[65] = "European";
    words[66] = "exactly";

    words[67] = "face";
    words[68] = "family";
    words[69] = "fashion";
    words[70] = "father";
    words[71] = "feeling";
    words[72] = "fifteen";
    words[73] = "finance";
    words[74] = "flower";
    words[75] = "focus";
    words[76] = "food";
    words[77] = "forever";
    words[78] = "forward";
    words[79] = "friendly";
    words[80] = "frequently";
    words[81] = "furniture";

    words[82] = "garage";
    words[83] = "garlic";
    words[84] = "gender";
    words[85] = "German";
    words[86] = "girlfriend";
    words[87] = "global";
    words[88] = "golden";
    words[89] = "greatest";
    words[90] = "guarantee";

    words[91] = "hand";
    words[92] = "have";
    words[93] = "health";
    words[94] = "helicopter";
    words[95] = "highlight";
    words[96] = "history";
    words[97] = "honest";
    words[98] = "however";
    words[99] = "hungry";

    words[100] = "idea";
    words[101] = "illegal";
    words[102] = "immediately";
    words[103] = "implement";
    words[104] = "importance";
    words[105] = "include";
    words[106] = "index";
    words[107] = "Indian";
    words[108] = "install";

    words[109] = "Japanese";
    words[110] = "Jewish";
    words[111] = "judge";
    words[112] = "juice";
    words[113] = "just";
    words[114] = "junior";
    words[115] = "joke";
    words[116] = "job";
    words[117] = "join";

    // words[118] = "idea";
    // words[119] = "illegal";
    // words[120] = "immediately";
    // words[121] = "implement";
    // words[122] = "importance";
    // words[123] = "include";
    // words[124] = "index";
    // words[125] = "Indian";
    // words[126] = "install";

    // words[127] = "Japanese";
    // words[128] = "Jewish";
    // words[129] = "judge";
    // words[130] = "juice";
    // words[131] = "just";
    // words[132] = "junior";
    // words[133] = "joke";
    // words[134] = "job";
    // words[135] = "join";
}

//Fill in the array
setWords();
//Fill in the output section
displayWords();

//Overlay funcitons, not related to the project
const on = () => document.getElementById("overlay").style.display = "block";
const off = () => document.querySelector("#overlay", "#introduction").style.display = "none";