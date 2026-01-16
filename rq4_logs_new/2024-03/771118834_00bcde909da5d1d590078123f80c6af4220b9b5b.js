const container = document.getElementById('container');
const registerBtn = document.getElementById('register');
const loginBtn = document.getElementById('login');

registerBtn.addEventListener('click', () => {
    container.classList.add('active')
});
loginBtn.addEventListener('click', () => {
    container.classList.remove('active')
});

const passwordInputLogin = document.getElementById('loginPassword');
const showPasswordCheckBox = document.getElementById('showLoginPassword');
showPasswordCheckBox.addEventListener('change', function () {
    if (this.checked) {
        passwordInputLogin.type = "text";
    }
    else {
        passwordInputLogin.type = "password";
    }
});


const passwordInput = document.getElementById('signUpPassword');
const showSignUpPasswordCheckBox = document.getElementById('showSignUpPassword');
showSignUpPasswordCheckBox.addEventListener('change', function () {
    if (this.checked) {
        passwordInput.type = "text";
    }
    else {
        passwordInput.type = "password";
    }
});



const reEnteredPassword = document.getElementById('Re-Enter-Password');
const showReEnteredPasswordCheckBox = document.getElementById('showReEnteredPassword');
showReEnteredPasswordCheckBox.addEventListener('change', function () {
    if (this.checked) {
        reEnteredPassword.type = "text";
    }
    else {
        reEnteredPassword.type = "password";
    }
});

const Password1 = document.getElementById('signUpPassword');
// console.log(Password1);
const Password2 = document.getElementById('Re-Enter-Password');
// console.log(Password2);

