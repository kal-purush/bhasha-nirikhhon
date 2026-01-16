var rnd;
var pos; //store position of the player in the game


function randomDice() //generates result of the dice roll 
{
 rnd=Math.floor(Math.random()*6)+1; //generates number between 1-6
var ip2="./images/dice"+rnd.toString()+".png"; //creates the image address of the dice that is drawn
document.querySelector("img1").setAttribute("src",ip2);
pos=pos+rnd; //keeps track of the position
console.log(pos);
}

function randomPosGen() //generates a random position frm 1-6
{
   return Math.floor(Math.random()*5)+1;

}

var snakePos=[randomPosGen()+4,randomPosGen()+4]; //generates snakes position
var ladderPos=[randomPosGen(),randomPosGen()]; //generates ladders position
var p;
$(".dce").click(function() 
{
    p=pos;
    animatePress("dce");
 randomDice();
 snkOrLddr();
 positionChange(p,pos);

   

   
})
function snkOrLddr() //checks if user encounters snake or ladder
{
for(var a=0;a<2;a++) //since we have 2 snakes/ladders we are running loop 2 times
{
if(snakePos[a]==ladderPos[a])
{
    snakePos[a]=ladderPos[a]+1;
}
else
{
    if(snakePos[a]==pos) //encounter snake
    {
    pos=pos-Math.floor(Math.random()*3);//we are downgrading players upto 2 steps
    alert("SNAKE ENCOUNTERED , YOU HAVE BEEN DOWNGRADED TO "+pos);}
    else if(ladderPos[a]==pos) //encounter ladder
    {
        pos=pos+Math.floor(Math.random()*3); //upgrading players position upto 2 steps
        alert("LADDER ENCOUNTERED , YOU HAVE BEEN UPGRADED TO"+pos);}
        
    }
}}
function animatePress(currentColor) { //to provide a press effect
    $("#" + currentColor).addClass("pressed");
    setTimeout(function () {
      $("#" + currentColor).removeClass("pressed");
    }, 400);
  }
  function positionChange(strt,end) //takes initial and final position after a dice roll to animate pos chang
  { if(strt<end)//if users encounter a ladder or just normal dice roll
    {
    for(var p=strt;p<=end;p++)
    {
    $("#" + p).addClass("pressed");
    setTimeout(function () {
      $("#" + p).removeClass("pressed"); //animates the movement of player
    }, 100);
    if(p==end) // to mark the final position after a dice roll 
    $("#" + p).addClass("pressed");}}
else
{
    for(var p=strt;p>=end;p--) //if user encounters ladder
    {
    $("#r" + p).addClass("pressed");
    setTimeout(function () {
      $("#r" + p).removeClass("pressed");
    }, 100);
    if(p==end)
    $("#r" + p).addClass("pressed");}
}
  }
  