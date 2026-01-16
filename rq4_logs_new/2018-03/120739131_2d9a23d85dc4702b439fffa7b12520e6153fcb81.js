const rock = document.querySelector("#rock");
const paper = document.querySelector("#paper");
const scissors = document.querySelector("#scissors");
const player = document.querySelector('#player-selector button');
const mouse_input = document.querySelector('#mouse-input');
const announcement = document.querySelector('.announcement')
const computerPoint = document.querySelector('.computer .score');
const playerPoint = document.querySelector('.player .score');
const winningSound = new Audio('Sounds/winningSound.mp3');
const losingSound = new Audio('Sounds/losingSound.wav');
const playerTitle = document.querySelector('#btntitle');
const playerImage = document.querySelector('#player-selector .btnimage');
const computerImage = document.querySelector('#computer-selector .btnimage'); 

const rock_left = rock.offsetLeft;
const paper_top = paper.offsetTop;
const scissors_left = scissors.offsetLeft;

let player_point = 0;
let computer_point = 0;

function showOption(){
    rock.style.left = '120px';
    paper.style.top = '330px';
    scissors.style.left = '380px';
}
function changBackground(player,computer){
    playerTitle.style.display = 'none';
    playerImage.style.display = 'inline-block';
    if(player == 'Rock')  playerImage.src = 'icons/Rock.png';
    else if(player == 'Paper')  playerImage.src = 'icons/Paper.png';
    else if( player == 'Scissors')  playerImage.src = 'icons/Scissors.png';

    computerImage.style.display = 'inline-block';
    if( computer == 'Rock')  computerImage.src = 'icons/Rock.png';
    else if( computer == 'Paper')  computerImage.src = 'icons/Paper.png';
    else if( computer == 'Scissors')  computerImage.src = 'icons/Scissors.png';
}

function handleGamePlay(playerChoice){
    const computerChoice = computerPlay();
    changBackground(playerChoice,computerChoice);
    if(playerChoice == computerChoice)  handleResult('Draw',playerChoice,computerChoice);
    else if(playerChoice == 'Paper' && computerChoice == 'Rock')    handleResult('Win',playerChoice,computerChoice);
    else if(playerChoice == 'Paper' && computerChoice == 'Scissors')    handleResult('Lose', playerChoice, computerChoice);
    else if(playerChoice == 'Rock' && computerChoice == 'Scissors')    handleResult('Win',playerChoice,computerChoice);
    else if(playerChoice == 'Rock' && computerChoice == 'Paper')    handleResult('Lose', playerChoice, computerChoice);
    else if(playerChoice == 'Scissors' && computerChoice == 'Paper')    handleResult('Win',playerChoice,computerChoice);
    else if(playerChoice == 'Scissors' && computerChoice == 'Rock')    handleResult('Lose', playerChoice, computerChoice);
}

function handleResult(result, playerChoice, computerChoice)
{
    announcement.innerHTML = `You ${result}, ${playerChoice} against ${computerChoice}`;

    (result == 'Win') ? ++player_point : ++computer_point;
    playerPoint.innerHTML = `${player_point}`;
    computerPoint.innerHTML = `${computer_point}`;

    (result == 'Win') ? winningSound.play() : losingSound.play();
}
function computerPlay(){
    const randomIndex = Math.floor(Math.random() * 3);
    const Choices = ['Rock', 'Paper', 'Scissors'];
    return Choices[randomIndex];
}

function resetOption(){
    if(document.activeElement != player){
        rock.style.left =   rock_left + 'px';
        paper.style.top =   paper_top + 'px';
        scissors.style.left =   scissors_left + 'px';
    }
}
player.addEventListener('click', showOption);
document.addEventListener('click',resetOption);
rock.addEventListener('click', function() { handleGamePlay('Rock'); });
paper.addEventListener('click', function() { handleGamePlay('Paper'); });
scissors.addEventListener('click', function() { handleGamePlay('Scissors'); });