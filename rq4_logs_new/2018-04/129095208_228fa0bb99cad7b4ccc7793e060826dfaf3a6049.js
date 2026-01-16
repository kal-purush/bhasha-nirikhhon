const leftpad = require('leftpad')
const CryptoJS = require('crypto-js')

const test = "EF797C8118F02DFB649607DD5D3F8C7623048C9C063D532CC95C5ED7A898A64F"


for(let num = 1; num <= 99999999; num++) {
    let nombre = leftpad(num, 8)
    let hash = CryptoJS.SHA256(nombre).toString(CryptoJS.enc.hex).toUpperCase()
    if (test == hash) {
        console.log('=======================')
        console.log("Original number is: " + num)
        console.log('=======================')
    } 
}