// Assignment Code
var generateBtn = document.querySelector("#generate");

//Declare array for each requirement
var passwordLength = 0;
var lower = ["a", 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'q', 'w', 'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'z', 'x', 'c', 'v', 'b', 'n', 'm'];
var upper = ['A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P'];
var number = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0'];
var symbols = ["<", ">", "?", "/", "]", "+", "=", "_", "-", "(", ")", "*", "&", "^", "%", "$", "#", "@", "!", "~", "`", ".", "|", ";", ":"];
// declare all the choice need to make
var lowerCharChoice;
var upperCharChoice;
var numberChoice;
var symbolsChoice;

//start work on funtion genratePassword
function generatePassword() {
  //save passwordLength input into passwordLength variable to used later
  passwordLength = Number(prompt("Please enter the number of character for your password:", 8));
  console.log('pass length' + passwordLength);

  //validate the passwordLength, make sure it meet the require
  if (passwordLength < 8 || passwordLength > 128) {
    passwordLength = prompt('Invalid. Please try again:');
    console.log("Password length " + passwordLength);
  }
  else if (!passwordLength) {
    passwordLength = prompt('Invalid. This need to be a number. Please try again:');
  }
  else if (Number.isNaN(passwordLength)) {
    console.log('numberpass', Number(passwordLength));
    passwordLength = prompt('Invalid. Password length should be a number');

  }
  else {
    alert(`Your password will have ${passwordLength} characters.`);
  }
  //save the user choices for lower char, upperchar, symbolschar, special character to variables
  lowerCharChoice = confirm('Do you want to add lowercase in your password?');
  console.log('lower case' + lowerCharChoice);
  upperCharChoice = confirm('Do you want to add uppercase in your password?');
  console.log('uppercase' + upperCharChoice);
  numberChoice = confirm('Do you want to add number in your password?');
  console.log('number' + numberChoice);
  symbolsChoice = confirm('Do you want to add symbol in your password?');
  console.log('symbols', symbolsChoice);
  //make sure at least one character choice be choosen
  while (!lowerCharChoice && !upperCharChoice &&
    numberChoice && !symbolsChoice) {
    alert('Password must have at least have one special character');
    lowerCharChoice = confirm('Do you want to add lowercase in your password?');
    upperCharChoice = confirm('Do you want to add uppercase in your password?');
    numberChoice = confirm('Do you want to add number in your password?');
    symbolsChoice = confirm('Do you want to add symbol in your password?');
  }
  //declare the characterIncluded as an array
  var characterIncluded = [];
  //concatinate the characterIncluded to any choosen character choices
  if (lowerCharChoice) {
    characterIncluded = characterIncluded.concat(lower);
  }
  if (upperCharChoice) {
    characterIncluded = characterIncluded.concat(upper);
  }
  if (numberChoice) {
    characterIncluded = characterIncluded.concat(number);
  }
  if (symbolsChoice) {
    characterIncluded = characterIncluded.concat(symbols);
  }
  //console.log('number', characterIncluded);

  // creat an empty var for password
  var randomPassword = "";
  //run the below block passwordLength times, each time pick up a random character within the characterIncluded and add it to randomPassword
  for (var i = 0; i < passwordLength; i++) {
    randomPassword += characterIncluded[Math.floor(Math.random() * characterIncluded.length)];
    //console.log(randomPassword)
  }
  return randomPassword;
}
// Write password to the #password input
function writePassword() {
  var password = generatePassword();
  //console.log('pass', password);
  var passwordText = document.querySelector("#password");
  //console.log('84 pass', passwordText);
  passwordText.value = password;
}
// Add event listener to generate button
generateBtn.addEventListener("click", writePassword);