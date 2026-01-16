const errorIcon = document.querySelectorAll(".error-icon");
const errorText = document.querySelectorAll(".error-text");
const button = document.getElementById("button");
const form = document.getElementById("form");
const input = Array.from(document.querySelectorAll(".input"))

const emailRegex = /^(([^<>()\[\]\\.,:\s@"]+(\.[^<>()\[\]\\.,:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/

const formValidate = () => {
    form.addEventListener("submit", (e) => {
        e.preventDefault()
    })
}

button.addEventListener("click", () => {
    if (input[0].value.trim().length < 1) {
        input[0].previousElementSibling.classList.remove("hidden")
        input[0].nextElementSibling.classList.remove("hidden")
        input[0].classList.add("invalid")
    }else {
        input[0] = true
    }
})

button.addEventListener("click", () => {
    if (input[1].value.trim().length < 1) {
        input[1].previousElementSibling.classList.remove("hidden")
        input[1].nextElementSibling.classList.remove("hidden")
        input[1].classList.add("invalid")
    }else {
        input[1] = true
    }
})

button.addEventListener("click", () => {
    if (input[2].value.trim().length < 1 || !emailRegex.test(input[2].value)) {
        input[2].previousElementSibling.classList.remove("hidden")
        input[2].nextElementSibling.classList.remove("hidden")
        input[2].classList.add("invalid")
    }else {
        input[2] = true
    }
})

button.addEventListener("click", () => {
    if (input[3].value.trim().length < 1) {
        input[3].previousElementSibling.classList.remove("hidden")
        input[3].nextElementSibling.classList.remove("hidden")
        input[3].classList.add("invalid")
    }else {
        input[3] = true
    }
})


input[0].addEventListener("change", () => {
    if (input[0].value.trim().length > 0) {
        input[0].previousElementSibling.classList.add("hidden")
        input[0].nextElementSibling.classList.add("hidden")
        input[0].classList.remove("invalid")
    }
})

input[1].addEventListener("change", () => {
    if (input[1].value.trim().length > 0) {
        input[1].previousElementSibling.classList.add("hidden")
        input[1].nextElementSibling.classList.add("hidden")
        input[1].classList.remove("invalid")
    }
})

input[2].addEventListener("change", () => {
    if (emailRegex.test(input[2].value)) {
        input[2].previousElementSibling.classList.add("hidden")
        input[2].nextElementSibling.classList.add("hidden")
        input[2].classList.remove("invalid")
    }
})

input[3].addEventListener("change", () => {
    if (input[3].value.trim().length > 0) {
        input[3].previousElementSibling.classList.add("hidden")
        input[3].nextElementSibling.classList.add("hidden")
        input[3].classList.remove("invalid")
    }
})

button.addEventListener("click", () => {
    for (let i = 0; i < input.length; i++) {
        if (input[0] == true && input[1] == true && input[2] == true && input[3] == true) {
            form.submit()
        }else {
            formValidate()
        }
    }
})


