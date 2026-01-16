const form = document.getElementById('form')
const email = document.getElementById('email')
const emailconfirmation = document.getElementById('email-confirmation')
const password = document.getElementById('password')
const passwordconfirmation = document.getElementById('password-confirmation')

form.addEventListener("submit", (e) => {
    e.preventDefault();

    checkInputs();
});

function checkInputs(){
    const emailValue = email.value
    const emaiconfirmationlValue = emailconfirmation.value
    const passwordValue = password.value
    const passwordconfirmationValue = passwordconfirmation.value

    if(emailValue == ''){
        setErrorFor(email, "O email é obrigatório")
    }
}

function setErrorFor(input, message){
    const formControl = input.parentElement;
    const small = formContro.querySelector("small");

    small.ineerText = message;

    formControl.className = 'form-control error';
}

function serSuccessFor(input){
    const formControl = input.parentElement;
    formControl.className = "form-control success";
}