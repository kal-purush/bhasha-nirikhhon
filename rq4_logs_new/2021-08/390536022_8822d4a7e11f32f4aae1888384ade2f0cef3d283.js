const questionsRapid = [{
    "question": "Clam Chowder originated in which country?",
    "answer": ["Australia", "United Kingdom", "USA", "Spain"],
    "final": "USA",
    "image": "./images/Earth.png"
},
{
    "question": "What is this cut of meat called?",
    "answer": ["Chop", "Wing", "Nugget", "Steak"],
    "final": "Steak",
    "image": "./images/Meat.png" 
},
{
    "question": "How many secret herbs and spices are there in the Colonel's recipe?",
    "answer": ["Eleven", "At least a million", "Infinity", "Zero"],
    "final": "Eleven",
    "image": "./images/KFC.png"
},
{
    "question": "What essential nutrient do you get from meat, poultry, eggs, and fish?",
    "answer": ["Caffeine", "Protein", "Peanut Butter", "Gainz"],
    "final": "Protein",
    "image": "./images/MeatFish.png"
},
{
    "question": "What bean that originated in East Asia is used for miso, oil, and tofu?",
    "answer": ["Lima Bean", "Magic Bean", "Peanut", "Soy Bean"],
    "final": "Soy Bean",
    "image": "./images/Ramen.png"
},
{
    "question": "Who is this celebrity chef?",
    "answer": ["Max Robbins", "Gordon Ramsay", "The Swedish Chef", "Emeril Lagasse"],
    "final": "The Swedish Chef",
    "image": "./images/SC.png"
},
{
    "question": "What is the main ingredient in an omelette du fromage?",
    "answer": ["Cheese", "Love", "Peppers", "Egg Shells"],
    "final": "Cheese",
    "image": "./images/Eggs.png"
},
{
    "question": "Who is the famous mascot for McDonald's?",
    "answer": ["Ronald McDonald", "Jon Snow", "Caillou", "Naruto"],
    "final": "Ronald McDonald",
    "image": "./images/RM.png"
},
{
    "question": "What is the name of this vegetable?",
    "answer": ["Broccoli", "Bok-Choy", "Carrot", "Hearty Radish"],
    "final": "Bok-Choy",
    "image": "./images/Bok.png"
},
{
    "question": "What is this ingredient called?",
    "answer": ["No Idea", "Chicken of the Sea", "Scallops", "Bananas"],
    "final": "Scallops",
    "image": "./images/Scallops.png"
},
{

"question": "What is the most consumed manufactured drink in the world?",
"answer": ["Tea", "Beer", "Coca-cola", "Pepsi"],
"final": "Tea",
"image": "./images/Beverages.png"
},
{
"question": "A macaron is typically made using which nut?",
"answer": ["Almonds", "Pistachios", "Peanuts", "Cashews"],
"final": "Almonds",
"image": "./images/Macaron.png"
},
{
"question": "In which country did Tea originate?",
"answer": ["Japan", "Thailand", "United Kingdom", "China"],
"final": "China",
"image": "./images/Tea.png"
},
{
"question": "Where did Mulligatawny soup originate?",
"answer": ["Malaysia", "India", "Thailand", "China"],
"final": "India",
"image": "./images/Mulligatawny.jpeg"
},
{
"question": "Which country has the wine making region known as the Barossa Valley?",
"answer": ["France", "Australia", "Germany", "Italy"],
"final": "Australia",
"image": "./images/Wine.png"
},
{
"question": "What is actually a fruit, not a veggie?",
"answer": ["Broccoli", "Tomato", "Potato", "Cauliflower"],
"final": "Tomato",
"image": "./images/FV.png"
},
{
"question": "What ingredient makes a Martini \"Dirty\"",
"answer": ["Finesse", "Olive Brine", "Onion Juice", "Bleu Cheese"],
"final": "Olive Brine",
"image": "./images/Martini.png"
},
{
    "question": "If you were in Australia and eating a 'sinker' what would you be having?",
    "answer": ["Spaghetti", "Pie", "Fries", "Soup"],
    "final": "Pie",
    "image": "./images/Australia.png"
},
{
    "question": "If you were having a cocktail of Guinness and Champagne what would it be called?",
    "answer": ["Red Velvet", "Blue Velvet", "Black Velvet", "Gold Velvet"],
    "final": "Black Velvet",
    "image": "./images/Guinness.png"
},
{
    "question": "What fruit is traditionally used in an Eve's Pudding?",
    "answer": ["Pineapples", "Oranges", "Strawberries", "Apples"],
    "final": "Apples",
    "image": "./images/evepudding.jpeg"
},
{
    "question": "Which country is the origin of the beef stew known as Daube?",
    "answer": ["Austria", "Sweden", "France", "Germany"],
    "final": "France",
    "image": "./images/Daube.jpeg"
},
{
    "question": "What fast food restaurant is credited with introducing the first modern-day drive-thru window?",
    "answer": ["Mickey D's", "Wendy's", "Burger King", "Taco Bell"],
    "final": "Wendy's",
    "image": "./images/fastfood.png"
},
{
    "question": "Which of the following is in the cruciferous vegetable family?",
    "answer": ["Cabbage", "Tomato", "Cucumber", "Potato"],
    "final": "Cabbage",
    "image": "./images/veggies.png"
},
{
    "question": "What is the common name for Allium sativum?",
    "answer": ["Tomato", "Garlic", "Kale", "Watermelon"],
    "final": "Garlic",
    "image": "./images/veggies.png"
},
{
    "question": "Which of the following is not considered a \"Mother Sauce\"?",
    "answer": ["Hollandaise", "Bechamel", "Veloute", "Bearnaise"],
    "final": "Bearnaise",
    "image": "./images/ketchup.png"
},
{
    "question": "Where did \"Spam\" originate?",
    "answer": ["Hawaii", "Mars", "Minnesota", "Tennessee"],
    "final": "Minnesota",
    "image": "./images/Spam.png"
},
]

//////create all elements
let body = document.querySelector('body');
//homepage modal container
let hPContainer = document.createElement('div');
document.body.appendChild(hPContainer);
hPContainer.setAttribute('class', 'container hP');
////homepage modal
let homePage = document.createElement('div');
hPContainer.appendChild(homePage);
homePage.setAttribute('class', 'content homePage');
//welcome message
let welcomeMessage = document.createElement('h1');
homePage.appendChild(welcomeMessage);
welcomeMessage.setAttribute('class', 'welcome');
welcomeMessage.innerHTML = "Welcome to Max's Food Trivia Game"
//game description
let gameDes = document.createElement('h2');
homePage.appendChild(gameDes);
gameDes.setAttribute('class', 'description');
gameDes.innerHTML = `Test Your Food Knowledge Against The Clock!`
//enter name
let nameInput = document.createElement('input');
homePage.appendChild(nameInput);
nameInput.setAttribute('class', 'input');
nameInput.setAttribute('placeHolder', 'Enter Your Name');
//start game button div
let startDiv = document.createElement('div');
homePage.appendChild(startDiv);
startDiv.setAttribute('class', 'buttonBox');
//start game button
let startGame = document.createElement('button');
startDiv.appendChild(startGame);
startGame.setAttribute('class', 'button start');
startGame.innerHTML = "Start Questions"
//back to Home button
let backHome = document.querySelector('.home')
startDiv.appendChild(backHome);
/////Trivia Modal Container
let tContainer = document.createElement('div');
document.body.appendChild(tContainer);
tContainer.setAttribute('class', 'container tRCont')
////Trivia Modal
let trivia = document.createElement('div');
tContainer.appendChild(trivia);
trivia.setAttribute('class', 'content trivia');
//question - inserted randomly 
let question = document.createElement('h1');
trivia.appendChild(question);
question.setAttribute('class', 'question');
question.innerHTML = "Sample Question Here"
//picture - inserted with question
let questionPic = document.createElement('img');
trivia.appendChild(questionPic);
questionPic.setAttribute('class', 'questionPic');
//answer button div
let answerCont = document.createElement('div');
trivia.appendChild(answerCont);
answerCont.setAttribute('class', 'ansCont');
//answer buttons - inserted with question
for(let i =0; i < 4; i++) {
    let answerBTN = document.createElement('button');
    answerCont.appendChild(answerBTN);
    answerBTN.setAttribute('class', 'button answer');
}
//nodelist with buttons in it
let answerButtons = document.querySelectorAll('.answer')
//round over modal container
let rOContainer = document.createElement('div');
document.body.appendChild(rOContainer);
rOContainer.setAttribute('class', 'container rOCont')
//round over modal
let roundOver = document.createElement('div');
rOContainer.appendChild(roundOver)
roundOver.setAttribute('class', 'content roundOver');
//Round over message
let roundOverMess = document.createElement('h1');
roundOver.appendChild(roundOverMess);
roundOverMess.setAttribute('class', 'rOMess');
roundOverMess.innerHTML = "Round Over"
//button to go to win screen when round is over
let goToWin = document.createElement('button');
roundOver.appendChild(goToWin);
goToWin.setAttribute('class', 'button showFinal')
goToWin.innerHTML = "Show Final Score!"
//scoreboard container
let sCContainer = document.createElement('div');
tContainer.appendChild(sCContainer);
sCContainer.setAttribute('class', 'sCCont')
//score board modal
let scoreBoard = document.createElement('div');
sCContainer.appendChild(scoreBoard);
scoreBoard.setAttribute('class', 'content scoreBoard');
//player name
let playerName = document.createElement('div');
scoreBoard.appendChild(playerName);
playerName.setAttribute('class', 'player');
playerName.innerHTML = "Player 1"
//total points div
let points = document.createElement('div');
scoreBoard.appendChild(points);
points.setAttribute('class', 'score points');
points.innerHTML = `<strong>Total Points</strong>`;
//total points number
let pointsNum = document.createElement('div');
points.appendChild(pointsNum);
pointsNum.setAttribute('class', 'pointsNum')
pointsNum.innerHTML = 0;
/////Win Screen Container
let wSContainer = document.createElement('div');
document.body.appendChild(wSContainer);
wSContainer.setAttribute('class', 'container wSCont')
////Win Screen Modal
let winScreen = document.createElement('div');
wSContainer.appendChild(winScreen);
winScreen.setAttribute('class', 'content winScreen');
//Nice Job/Better Luck Next Time Message
let endMessage = document.createElement('h1');
winScreen.appendChild(endMessage);
endMessage.setAttribute('class', 'endMessage');
//image- good/bad
let endImage = document.createElement('img');
winScreen.appendChild(endImage);
endImage.setAttribute('class', 'endImage');
//button for play again- brings back to home
let playAgain = document.createElement('button');
winScreen.appendChild(playAgain);
playAgain.innerHTML = "Play Again!"
playAgain.setAttribute('class', 'button playAgain');
let allButtons = document.querySelectorAll('.button')
//countdown timer container
let countDownContainer = document.createElement('div');
countDownContainer.setAttribute('class', 'content countDown');
tContainer.appendChild(countDownContainer)
//countdown timer modal
let countDown = document.createElement('div');
countDownContainer.appendChild(countDown);
countDown.setAttribute('class', 'content countDown');
//sound for right answer
let rightAudio = document.querySelector('#right');
//sound for wrong answer
let wrongAudio = document.querySelector('#wrong');

//countdown timer
let timer;
let timeLeft = 30;
//function to stop the game when the timer hits 0
function stopGame () {
    tContainer.classList.toggle('show-modal');
    rOContainer.classList.toggle('show-modal');
    clearInterval(timer)
}
//function for the countdown, and stops when it gets to 0
function countingDown () {
    timeLeft = timeLeft - 1;
    if(timeLeft >= 0){
        countDown.innerHTML = (timeLeft);
    }
    else{
        stopGame();
    }
}
//score counter
let pointsPlus = 0;
//array to hold numbers for random question generator
let questionArray = [];
//modal functionality 

// function that shows the homepage when it is loaded
function loadHome () {
       hPContainer.classList.toggle('show-modal')
}


//randomquestion function that does not repeat questions
function randomQuestion () {
    if (!questionArray.length) {
        for (let i = 0; i < questionsRapid.length; i ++) {
            questionArray.push(i);
    }
}
let random = Math.floor(Math.random() * questionArray.length);
let val = questionArray[random];
        questionArray.splice(random, 1);
        question.innerHTML = questionsRapid[val].question;
        questionPic.src = questionsRapid[val].image;
    for (let i = 0; i < answerButtons.length; i++) {
        answerButtons[i].innerHTML = questionsRapid[val].answer[i]
}   
}

////enter's name into playerName
////pulls up first question
function getStarted () {
    //puts the players name into the player object
        playerName.innerHTML = nameInput.value;
        player = playerName.innerHTML;
        if (!nameInput.value){
            alert('Please put in your name')
        }else if(nameInput.value){
        tContainer.classList.toggle('show-modal')
        hPContainer.classList.toggle('show-modal')
//populate first question into trivia modal
        randomQuestion()
//start the countdown timer       
        timer = setInterval(countingDown, 1000);
        countingDown();

}
}



//answer buttons take you to the right answer modal if it is the right answer
function chooseAnswer (event) {

//logic for guessing right or wrong answer and increases numbers on scoreboard
//either brings up right modal or wrong modal
let isCorrect = questionsRapid.map(item => event.target.innerHTML == item.final)
        if (isCorrect.includes(true)) {
            rightAudio.play()
            pointsPlus += 2
    }   else {
            wrongAudio.play()
            pointsPlus -= 1
    }
//adds the points to the scoreboard
        pointsNum.innerHTML = pointsPlus;
        randomQuestion()
}


//function to go to the win screen from the round over screen
function winner  ()  {
    wSContainer.classList.toggle('show-modal')
    sCContainer.classList.toggle('show-modal')
    rOContainer.classList.toggle('show-modal')
  
   
    wSContainer.appendChild(sCContainer)
    if (pointsNum.innerHTML > 10) {
        endMessage.innerHTML = `Great Job ${playerName.innerHTML}, You Scored ${pointsNum.innerHTML} Points!`
    } else if(pointsNum.innerHTML < 5) {
        endMessage.innerHTML = `Yikes ${playerName.innerHTML}, You Scored ${pointsNum.innerHTML} Points. Hit the Books`
    }
}


//function to bring you from win screen back to homepage
function replay () {
    loadHome()
    wSContainer.classList.toggle('show-modal');
    tContainer.appendChild(sCContainer);
    pointsNum.innerHTML = 0;
    pointsPlus = 0;
    
}


//event propagation for all buttons
function moveModal (event) {

    if(event.target === startGame) {
        getStarted()
    }else if(event.target === goToWin) {
        winner()
    }else if(event.target === playAgain) {
        replay()
    }
    }
    
//event listeners
document.addEventListener('load',loadHome())
allButtons.forEach(button => button.addEventListener('click', moveModal))
answerButtons.forEach(button => button.addEventListener('click',chooseAnswer))