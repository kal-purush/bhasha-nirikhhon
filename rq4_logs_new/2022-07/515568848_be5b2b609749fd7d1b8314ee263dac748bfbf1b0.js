let scoreHomeEl = document.getElementById("score-home")
let scoreGuestEl = document.getElementById("score-guest")
let newScoreHome = 0;
let newScoreGuest = 0;

function add1Home() {
    newScoreHome += 1
    scoreHomeEl.textContent = newScoreHome;
}

function add2Home() {
    newScoreHome += 2
    scoreHomeEl.textContent = newScoreHome;
}

function add3Home() {
    newScoreHome += 3
    scoreHomeEl.textContent = newScoreHome;
}

function add1Guest() {
    newScoreGuest += 1
    scoreGuestEl.textContent = newScoreGuest;
}

function add2Guest() {
    newScoreGuest += 2
    scoreGuestEl.textContent = newScoreGuest;
}

function add3Guest() {
    newScoreGuest += 3
    scoreGuestEl.textContent = newScoreGuest;
}