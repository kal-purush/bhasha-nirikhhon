let inputDir = {x:0,y:0};
let speed = 5;
var flag = 0,temp = 0;
let lastTime = 0;
let snakeArray = [{x:3,y:2}];
let food = {x:7,y:9};
let time = 0;
let GameOverSound = new Audio("gameover.mp3");
let GameSound = new Audio("music.mp3");
let FoodSound = new Audio("food.mp3");
let MoveSound = new Audio("move.mp3");
let Score = 0;
let top1 = document.body.getElementsByTagName("button")[0];
// Game functions ithe
const MoveUp = () =>{
     GameSound.pause();
     flag = 1;
               Music();
               inputDir.x = 0;
               inputDir.y = -1;
               top1.style.background = "#93CC42";
               left.style.background="transparent";
               right.style.background="transparent";
               bottom.style.background="transparent";
}
const MoveRight = () =>{
     GameSound.pause();
     flag = 1;
               Music();
               inputDir.x = 1;
               inputDir.y = 0;
               right.style.background = "#93CC42";
               left.style.background="transparent";
               top1.style.background="transparent";
               bottom.style.background="transparent";
}
const MoveDown = () =>{
     GameSound.pause();
     flag = 1;
               Music();
               inputDir.x = 0;
               inputDir.y = 1;
               bottom.style.background = "#93CC42";
               left.style.background="transparent";
               right.style.background="transparent";
               top1.style.background="transparent";
}
const MoveLeft = () =>{
     GameSound.pause();
     flag=1;
               Music();
               inputDir.x = -1;
               inputDir.y = 0;
               left.style.background = "#93CC42";
               top1.style.background="transparent";
               right.style.background="transparent";
               bottom.style.background="transparent";
}
const Music = () =>
{
     if(flag == 1 && temp == 0)
     {
          MoveSound.play();  
     }
     else if(flag == 2 &&temp == 0)
     {
          FoodSound.play();
     }
     else if(flag == 3 && temp == 0)
     {    
          GameSound.play();
     }
     else if(flag ==4 && temp == 4)
     {
          let Unmute = document.getElementsByClassName("unmute")[0];
          Unmute.classList.remove('mute');
          temp = 0;
     }
     else if(flag ==5 && temp == 5)
     {
          let Mute = document.getElementsByClassName("unmute")[0];
          Mute.classList.add('mute');    
          GameSound.pause();
          FoodSound.pause();
          MoveSound.pause();  
     }
     else if(flag ==6 && temp == 0)
     {
          GameOverSound.play();
          GameSound.play();
     }
}
const main = (ctime) =>
{
    window.requestAnimationFrame(main);
     if ((ctime-lastTime)/1000 <1/speed) {
          return;
     }
     lastTime = ctime;
     gameEngine();

}

const isCollide = (snakeArr) =>
{
     for(let i = 1;i<snakeArray.length;i++)
     {
          if(snakeArray[0].x === snakeArr[i].x && snakeArray[0].y === snakeArr[i].y)
          {
               return true;
          }
     }
          if(snakeArray[0].x > 18 || snakeArray[0].x <= 0 || snakeArray[0].y > 18 || snakeArray[0].y <= 0)
          {
               return true;
          }
          
}

const gameEngine = () =>
{
     //update snake kara
     if(isCollide(snakeArray))
     {
          inputDir = {x:0,y:0};
          flag =6;
          Music();
          right.style.background ="transparent";
          left.style.background="transparent";
          top1.style.background="transparent";
          bottom.style.background="transparent";
          alert("Game Over !! Your Score : "+Score);
          Score = 0;
          scorePoint.innerHTML = "Score : "+Score;
          snakeArray = [{x:13,y:15}];
     }
     // regenerate food here
if (snakeArray[0].x === food.x && snakeArray[0].y === food.y)
{
     flag = 2;
     Music();
     snakeArray.unshift({x: snakeArray[0].x+inputDir.x,y: snakeArray[0].y+inputDir.y});
     let a = 2,b=16;
  food = {x: Math.round(a+(b-a)*Math.random()),y: Math.round(a+(b-a)*Math.random())};
  
     Score += 10;
     scorePoint.innerHTML = "Score : "+Score;
     if(Score>highreval){
            highreval = Score;
            localStorage.setItem("hiscore", JSON.stringify(highreval));
            hiscoreBox.innerHTML = "High-Score: " + highreval;
        }
}
// Move snake
for (let i = snakeArray.length-2; i >=0; i--) {
     snakeArray[i+1] = {...snakeArray[i]};
}

snakeArray[0].x += inputDir.x;
snakeArray[0].y += inputDir.y;


     // Display the snake and food
board.innerHTML = "";
snakeArray.forEach((element,index) =>{
     let snakeElement = document.createElement('div');
     snakeElement.style.gridRowStart = element.y;
     snakeElement.style.gridColumnStart = element.x;
     if(index === 0)
     {
     snakeElement.classList.add('head');
     }
     else
     {
          snakeElement.classList.add('snake');
     }

     board.appendChild(snakeElement);
});
     foodElement = document.createElement('div');
     foodElement.style.gridRowStart = food.y;
     foodElement.style.gridColumnStart = food.x;
     foodElement.classList.add('food');
     board.appendChild(foodElement);
}

// main logic
let hiscore = localStorage.getItem("hiscore");
if(hiscore === null){
    highreval = 0;
    localStorage.setItem("hiscore", JSON.stringify(highreval))
}
else{
    highreval = JSON.parse(hiscore);
   hiscoreBox.innerHTML = "High-Score: " + hiscore;
}

window.requestAnimationFrame(main);
if(time==0){
setTimeout(function() {speed = 6}, 60000);
setTimeout(function() {speed = 7}, 120000);
setTimeout(function() {speed = 8}, 240000);
setTimeout(function() {speed = 9}, 300000);
}
time++;
window.addEventListener('keydown',element=>{
     // if (element.key == 'p') {
     //      inputDir = {x :0,y:-1};
     // }
     GameSound.pause();
     flag = 1;
     switch(element.key)
     {
          case 'w' :
          case 'W' :
          case 'ArrowUp' :
               Music();
               inputDir.x = 0;
               inputDir.y = -1;
               top1.style.background = "#93CC42";
               left.style.background="transparent";
               right.style.background="transparent";
               bottom.style.background="transparent";
               break;
          case 's' : 
          case 'S' : 
          case 'ArrowDown' : 
               Music();
               inputDir.x = 0;
               inputDir.y = 1;
               right.style.background ="transparent";
               left.style.background="transparent";
               top1.style.background="transparent";
               bottom.style.background="#93CC42";
               break;
          case 'a' : 
          case 'A' : 
          case 'ArrowLeft' : 
               Music();
               inputDir.x = -1;
               inputDir.y = 0;
               right.style.background = "transparent";
               left.style.background="#93CC42";
               top1.style.background="transparent";
               bottom.style.background="transparent";
               break;
          case 'd' :
          case 'D' :
          case 'ArrowRight' :
               Music();
               inputDir.x = 1;
               inputDir.y = 0;
               right.style.background = "#93CC42";
               left.style.background="transparent";
               top1.style.background="transparent";
               bottom.style.background="transparent";
               break;
          case 'u' : 
          case 'U' : 
               flag=4;
               temp = 4;
               Music();
          break;
          case 'm' : 
          case 'M' : 
               flag=5;
               temp = 5;
               Music();
          break;
          default :
          break;
     }
});