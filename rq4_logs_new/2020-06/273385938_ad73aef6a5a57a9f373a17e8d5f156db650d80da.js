var passLength = 0;
var CharTypeNum = "false";
var CharTypeUpper = "false";
var CharTypeLower = "false";
var CharTypeSpecial = "false";
var newPassword = [];

var generateBtn = document.querySelector("#generate")
var passwordText = document.querySelector("#password")

const typeLowerCaseLetters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'];
const typeUpperCaseLetters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'];
const typeNumbers = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
const typeSpecialCharacters = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '{', '}', ':', '"', '<', '>', '?', '"'];

generateBtn.addEventListener("click", function () {
    event.preventDefault();
    getUserInput();
    generateChoicesArray();
    generateNewPassword();
    choicesArray = [];
});

//user input
function getUserInput() {
    passLength = prompt("Select Length of 8-128 characters: ");
    CharTypeNum = confirm("Include Numbers? ");
    CharTypeUpper = confirm("Include Upper Case? ");
    CharTypeLower = confirm("Include Lower Case? ");
    CharTypeSpecial = confirm("Include Special Characters? ");
};


function generateChoicesArray() {
    if (CharTypeUpper == true)
        choicesArray = choicesArray.concat(typeUpperCaseLetters);
    if (CharTypeLower == true)
        choicesArray = choicesArray.concat(typeUpperCaseLetters);
    if (CharTypeNum == true)
        choicesArray = choicesArray.concat(typeNumbers);
    if (CharTypeSpecial == true)
        choicesArray = choicesArray.concat(typeSpecialCharacters);
};


function generateNewPassword() {
    if (choicesArray.length < typeNumbers.length || getLength < 8) {
        alert("You did not meet the minimum standard ")
    } else {
        for (i = 0; i < getLength; i++) {
            newPassword += choicesArray[Math.floor(Math.random() * (choicesArray.length - 1))];
        }
        password.textContent = newPassword;
    };
    newPassword = [];
} 