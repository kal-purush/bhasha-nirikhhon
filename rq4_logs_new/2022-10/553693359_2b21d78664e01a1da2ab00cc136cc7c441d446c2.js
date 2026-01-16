
const WON_MESSAGE = 'Hurray 🎈🎊🎈, You are a lucky person!';
const NUMLOW_MESSAGE = 'Your guess is too low! 📉 ';
const NUMHIGH_MESSAGE = 'Your guess is too high! 📈';
const OUT_OF_GUESSES_MESSAGE = 'You have run out of guesses! 😭';
const OUT_OF_RANGE = 'Please enter a number between 1 and 20 😡';
const NO_VAL='Please enter a value 🥱';



let luckyNumber=Math.floor((Math.random()*20)+1);
console.log(luckyNumber);
let score=10;
let high=0;
const checkButton=document.querySelector("#check");
const userInput=document.querySelector(".guess");
const displayMessage=document.querySelector("#msg");
const Mainpg=document.querySelector("main");
const leftsec=document.querySelector(".left");
const resetBtn=document.querySelector("#reset");
const scoreSection=document.querySelector("#score");
const highscore=document.querySelector('#highscore');
const emoji=document.querySelector("#mid-emoji");
scoreSection.innerText=score;


const check=(guess)=>
{
    if(guess===0)
    return NO_VAL;
    else if(guess<1 || guess>20)
    return OUT_OF_RANGE;
    else if(guess===luckyNumber)
    return WON_MESSAGE;
    else if(guess<luckyNumber)
    return NUMLOW_MESSAGE;
    else if(guess>luckyNumber)
    return NUMHIGH_MESSAGE;

};

const scoreProcess=(message)=>
{
   if(message===WON_MESSAGE)
    {
        leftsec.style.visibility='hidden';
        Mainpg.style.backgroundColor = '#367E18';
        if(high<score)
        {
          high=score;
        }
        highscore.innerText=high;
        emoji.innerText='😲';
    }

    if (message === NUMLOW_MESSAGE || message === NUMHIGH_MESSAGE) {
        score--;
        scoreSection.textContent = score;
        Mainpg.style.backgroundColor = '#DD5353';
        emoji.innerText='😞';
      }
    
      if (score === 0) {
        leftsec.style.visibility = 'hidden';
        displayMessage.textContent = OUT_OF_GUESSES_MESSAGE;
        Mainpg.style.backgroundColor = '#333';
        emoji.innerText='💀';
      }
      

};

checkButton.addEventListener('click', () => {
    const message = check(Number(userInput.value));
    displayMessage.textContent = message;
    scoreProcess(message);
  });
  
  resetBtn.addEventListener('click', () => {
    luckyNumber = Math.floor((Math.random() * 20)+1);
    console.log(luckyNumber);
    score = 10;
    scoreSection.textContent = score;
    
    emoji.innerText='?';
  
    Mainpg.style.backgroundColor = '#333';
  
    leftsec.style.visibility = 'visible';
    displayMessage.textContent = 'Start guessing...';
  
    userInput.value = '';
  });
