function signupDisplay() {
    const signupOverlay = document.getElementById('signupOverlay');
    signupOverlay.style.display = 'unset';
    const loginOverlay = document.getElementById('loginOverlay');
    loginOverlay.style.display = 'none';
}

function loginDisplay() {
    const signupOverlay = document.getElementById('loginOverlay');
    signupOverlay.style.display = 'unset';
    const loginOverlay = document.getElementById('signupOverlay');
    loginOverlay.style.display = 'none';
}

function hide() {
    const signupOverlay = document.getElementById('loginOverlay');
    signupOverlay.style.display = 'none';
    const loginOverlay = document.getElementById('signupOverlay');
    loginOverlay.style.display = 'none';
}