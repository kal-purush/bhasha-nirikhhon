
const state = {
    view: { // controle visual
        squares: document.querySelectorAll('.square'),
        enemy: document.querySelector('.enemy'),
        timeLeft: document.querySelector('#time-left'),
        score: document.querySelector('#score'),
    },
    values: { //controle de calculo
        timerId: null,
        gameVelocity: 500,
        hitPosition: 0,
        result: 0,
        currentTime: 16,
    },
    actions: { // como o proprio nome diz, ações
        countDownTimerId: setInterval(countDown, 1000),
    }
};

function countDown() {
    state.values.currentTime--;
    state.view.timeLeft.textContent = state.values.currentTime

    if (state.values.currentTime <= 0) {
        clearInterval(state.actions.countDownTimerId);
        clearInterval(state.values.timerId);
        alert(`Game Over! O seu resultado foi: ${state.values.result}`);
    }
}

function randomSquare() {
    state.view.squares.forEach((square) => {
        square.classList.remove('enemy');
    });
    let randomNumber = Math.floor(Math.random() * 9);
    let radomSquare = state.view.squares[randomNumber];
    radomSquare.classList.add('enemy');
    state.values.hitPosition = radomSquare.id;
}

function moveEnemy() {
    state.values.timerId = setInterval(randomSquare, state.values.gameVelocity);
}

function addListenerHitBox() {
    state.view.squares.forEach((square) => {
        square.addEventListener('mousedown', () => {
            if (square.id === state.values.hitPosition) {
                state.values.result++;
                state.view.score.textContent = state.values.result;
                state.values.hitPosition = null;
                playSound('hit.m4a');
            }
        })
    });
};

function playSound(audioName) {
    let audio = new Audio(`/src/audio/${audioName}`);
    audio.volume = 0.2;
    audio.play();
}

function init() { //initialize
    moveEnemy();
    addListenerHitBox();
}

init();