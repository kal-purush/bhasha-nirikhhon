"use strict";
let passwordlength;
let lowercase;
let uppercase;
let numbers;
let specialcharacters;

// Set the password length between 8 and 128.
function length() {
  passwordlength = prompt("Length of Your Password, >= 8 and <=128");
  if (passwordlength >= 8 && passwordlength <= 128) {
  } else {
    alert("repick your password length between 8 and 128!");
    length();
  }
}

// When asking character type, lower case will be included.
function with_lowercase() {
  lowercase = prompt("with Lowercase in password? yes or no");
  if (lowercase == "yes" || lowercase == "no") {
  } else {
    alert("Please answer yes or no, all lowercase");
    with_lowercase();
  }
}

// When asking character type, upper case will be included.
function with_uppercase() {
  uppercase = prompt("with Uppercase in password? yes or no");
  if (uppercase == "yes" || uppercase == "no") {
  } else {
    alert("Please answer yes or no, all lowercase");
    with_uppercase();
  }
}

// When asking character type, number will be included.
function with_numbers() {
  numbers = prompt("with Numbers in password? yes or no");
  if (numbers == "yes" || numbers == "no") {
  } else {
    alert("Please answer yes or no, all lowercase");
    with_numbers();
  }
}

// When asking character type, special character will be included.
function with_specialcharacters() {
  specialcharacters = prompt(
    "with special characters in password? yes or no"
  );
  if (specialcharacters == "yes" || specialcharacters == "no") {
  } else {
    alert("Please answer yes or no, all lowercase");
    with_specialcharacters();
  }
}

// Input should be validated and at least one character type should be selected
function checkyes() {
  with_lowercase();
  with_uppercase();
  with_numbers();
  with_specialcharacters();
  if (
    lowercase == "no" &&
    uppercase == "no" &&
    numbers == "no" &&
    specialcharacters == "no"
  ) {
    alert("You must answer at least one 'yes'!!!!!");
    checkyes();
  } else {
  }
}

// A password will generated that matches the selected criteria
function generatePassword() {
  const lowercaselist = "abcdefghijklmnopqrstuvwxyz";
  const uppercaselist = lowercaselist.toUpperCase();
  const numberlist = "0123456789";
  const specialcharlistincom = " !'()*+,-$#&%./:;<=>?@[]^_`{|}~";
  const specialcharlist = specialcharlistincom.concat('"');
  let finallist = "";
  let yourpassword = [];

  length();
  alert(
    "You must answer at least one 'yes' for the following password criteria"
  );
  checkyes();
  if (lowercase == "yes") {
    finallist = finallist.concat(lowercaselist);
  } else {
  }
  if (uppercase == "yes") {
    finallist = finallist.concat(uppercaselist);
  } else {
  }
  if (numbers == "yes") {
    finallist = finallist.concat(numberlist);
  } else {
  }
  if (specialcharacters == "yes") {
    finallist = finallist.concat(specialcharlist);
  } else {
  }


//generate the password, array
  for (let i = 0; i < passwordlength; i++) {
    yourpassword.push(finallist[Math.trunc(Math.random() * finallist.length)]);
  }

//randomly replace a special characters into the password
  if (specialcharacters == "yes") {
    yourpassword[Math.trunc(Math.random() * yourpassword.length)] =
      specialcharlist[Math.trunc(Math.random() * specialcharlist.length)];
  } else {
  }

  //convert array into string
  yourpassword = yourpassword.join("");
  alert(`your password is:${yourpassword}`);

  return yourpassword;
}

// Get references to the #generate element
var generateBtn = document.querySelector("#generate");

// Write password to the #password input
function writePassword() {
  var password = generatePassword();
  var passwordText = document.querySelector("#password");

  passwordText.value = password;

}

// Add event listener to generate button
generateBtn.addEventListener("click", writePassword);

console.log("copyofmain");