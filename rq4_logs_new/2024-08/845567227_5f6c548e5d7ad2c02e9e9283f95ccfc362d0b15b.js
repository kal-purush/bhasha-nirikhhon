
const urlParams = new URLSearchParams(window.location.search)
const AuthCode = urlParams.get("code")

console.log(`Code: ${ AuthCode }`)
let replace = document.querySelector('p')

replace.innerText = `Code: ${ AuthCode }`