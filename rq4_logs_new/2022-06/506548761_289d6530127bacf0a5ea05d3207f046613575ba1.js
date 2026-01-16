let characters = ["1234567890!@#$%^&*()_+,;[./']<:{>?}|qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM"]


let result1 = document.getElementById("results1")
let result2 = document.getElementById("results2")
let result3 = document.getElementById("results3")
let result4 = document.getElementById("results4")


function password() {
    var pass = ""
    let str = characters[0]

    for ( i = 0; i <=15; i++) {
        var char = Math.floor(Math.random() * str.length + 1)
        pass += str.charAt(char)
    }
    return pass
}

function passwordGen() {
    result1.textContent = password()
    result2.textContent = password()
    result3.textContent = password()
    result4.textContent = password()
}
