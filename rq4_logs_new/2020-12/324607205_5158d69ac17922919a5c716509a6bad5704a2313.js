const usernameError = document.getElementById('usernameError')
const passwordError = document.getElementById('passwordError')
const checkPasswordMatching = document.getElementById('checkPasswordMatching')
const form = document.getElementById('signUpForm')

form.addEventListener('submit', (e) => {
    if ((username.value.length < 8) || (username.value.length > 20)) {
        e.preventDefault()
    }

    if ((password.value.length < 10) || (password.value.length > 20)) {
        e.preventDefault()
    }

    if (checkPasswordMatching.innerText === 'Chưa chính xác !') {
        e.preventDefault()
    }
})


