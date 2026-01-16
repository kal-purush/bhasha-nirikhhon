const p1 = {
    score: 0,
    button: document.querySelector('#p1Button'),
    display:document.querySelector('#p1')
};


const p2 = {
    score: 0,
    button: document.querySelector('#p2Button'),
    display:document.querySelector('#p2')
};


let winningScore = 1;
let isGameOver = false;
const resetall = document.querySelector('#reset');
const winningScoreSelection = document.querySelector('#playto');


function updateScores(player, opponent){
    if (!isGameOver) {
        player.score += 1;
        if (player.score === winningScore) {
            isGameOver = true;
            p1.display.classList.add('has-text-success');
            p2.display.classList.add('has-text-danger');
            p1.button.disabled = true;
            p2.button.disabled = true;
        }
        player.display.textContent = player.score;
    }
}

p1.button.addEventListener('click',function(){
    updateScores(p1,p2);
});

p2.button.addEventListener('click',function(){
    updateScores(p2,p1);
});

winningScoreSelection.addEventListener('change', function () {
    winningScore = parseInt(this.value);
    reset();
})


resetall.addEventListener('click', reset);
function reset() {
    isGameOver = false;
    for(let p of [p1,p2]){
        p.score = 0;
        p.display.textContent = 0;
        p.display.classList.remove('has-text-success', 'has-text-danger');
        p.button.disabled = false;
    }
}
/*
    --------------- WITHOUT REFACTORING -------------------
let p1Score = 0;
let p2Score = 0;
const buttonp1 = document.querySelector('#p1Button');
const p1 = document.querySelector('#p1');
const buttonp2 = document.querySelector('#p2Button');
const p2 = document.querySelector('#p2');

buttonp1.addEventListener('click', function () {
    if (!isGameOver) {
        p1Score += 1;
        if (p1Score === winningScore) {
            isGameOver = true;
            // p1.style.color = 'green';
            // p2.style.color = 'red';
            p1.classList.add('has-text-success');
            p2.classList.add('has-text-danger');
            buttonp1.disabled = true;
            buttonp2.disabled = true;
        }
        p1.textContent = p1Score;

    }

});

buttonp2.addEventListener('click', function () {
    if (!isGameOver) {
        p2Score += 1;
        if (p2Score === winningScore) {
            isGameOver = true;
            // p2.style.color = 'green';
            // p1.style.color = 'red';
            p2.classList.add('has-text-success');
            p1.classList.add('has-text-danger');
            buttonp1.disabled = true;
            buttonp2.disabled = true;
        }
        p2.textContent = p2Score;

    }
});

function reset() {
    isGameOver = false;
    p1Score = 0;
    p2Score = 0;
    p1.textContent = 0;
    p2.textContent = 0;
    // p1.style.color = 'black';
    // p2.style.color = 'black';
    p1.classList.remove('has-text-success', 'has-text-danger');
    p2.classList.remove('has-text-success', 'has-text-danger');
    buttonp1.disabled = false;
    buttonp2.disabled = false;

    ---------------------------OR------------------------------------

    p1.score = 0;
    p2.score = 0;
    p1.display.textContent = 0;
    p2.display.textContent = 0;
    p1.display.classList.remove('has-text-success', 'has-text-danger');
    p2.display.classList.remove('has-text-success', 'has-text-danger');
    p1.button.disabled = false;
    p2.button.disabled = false;
}
*/

