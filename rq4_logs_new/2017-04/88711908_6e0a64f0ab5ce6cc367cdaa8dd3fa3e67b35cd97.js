  
var spread = [];

var vid;

var duration;
var cutVid;

var cuts = []; //simulate database
var currCut = 0;
var n = 10; // time of each clip
var spreadCount = 5;

var isPlaying = false;

function setup() { 
  // createCanvas(400, 400);

  cutVid = createVideo("bobross.mp4", initSpread);
  cutVid.size(720, 480);

  cutVid.style("margin", "-100px auto auto auto")
  cutVid.style("display", "block")
  
  var bob = select('.bob');
  bob.mousePressed(startSpread);
  
  // initSpread();
  //bobRoss button
  //on bobRoss click, go through spread and make each one visible
  //do logic to get random timestamps
  //on spread click, show video and play from that timestamp
  //on bobRoss click, return to original state. 
  //	clear timestamp array, make spread invisible, return bobross middle
  
  frameRate(1); //1 
} 

var counter = 0;
function draw()
{
  counter++;
  if(isPlaying)
  {
    if(counter % n === 0) //1 frame per second
    {

      if(currTime >= 0)
      {
        cutVid.time(cuts[currTime]);
      }

    }
  }

}
  

function initSpread()
{

  var past = select('#past');
  var present = select('#present');
  var future = select('#future');
  var reason = select('#reason');
  var potential = select('#potential');
  
  past.mousePressed(pastClicked);
  present.mousePressed(presentClicked);
  future.mousePressed(futureClicked);
  reason.mousePressed(reasonClicked);
  potential.mousePressed(potentialClicked);
  
  spread.push(present);
  spread.push(past);
  spread.push(future);
  spread.push(reason);
  spread.push(potential);

  cutVid.play();
}
var currPressed = -1;

function showVideo()
{
  
}

function getRandomTime()
{
  cuts = [];

  var fullTime = round(cutVid.duration());

  var numCardsInDeck = fullTime / n;

  var randomCard;

  var randomTime;

  for(var i = 0; i < spreadCount; i++)
  {
    randomCard = round(random(numCardsInDeck));

    randomTime = randomCard * n;

    cuts.push(randomTime)
  }

}

function startSpread()
{

  getRandomTime();

  if(isPlaying)
  {
    cutVid.stop();
    currTime = -1;
  }
  else
  {
    isPlaying = true;
  }
  
  //if video is visible, invisible, vice versa
  for(var i = 0; i < spread.length; i++)
  {
    spread[i].style("transform", "scale(1,1)");
    spread[i].style("opacity", "1");
  }


}

var currTime = -1;

function resetHighlighted()
{
  for(var i = 0; i < spread.length; i++)
  {
    spread[i].style("transform", "scale(1,1)");
  }

}

function cardRead(spreadIndex)
{
  resetHighlighted();
  counter = 0;
  spread[spreadIndex].style("transform", "scale(1.5,1.5)");
  currTime = spreadIndex;
  cutVid.play();
  cutVid.time(cuts[spreadIndex]);
}

function pastClicked()
{
  cardRead(1);
}

function presentClicked()
{
  cardRead(0);
}

function futureClicked()
{
 cardRead(2);
}

function reasonClicked()
{
  cardRead(3);
}

function potentialClicked()
{
  cardRead(4);
}


function keyPressed() {
  if (keyCode === 49) {
    presentClicked()
  } else if (keyCode === 50) {
    pastClicked()
  }
  else if (keyCode === 51) {
    futureClicked()
  }
  else if (keyCode === 52) {
    reasonClicked();
  }
  else if (keyCode === 53) {
    potentialClicked();
  }

}