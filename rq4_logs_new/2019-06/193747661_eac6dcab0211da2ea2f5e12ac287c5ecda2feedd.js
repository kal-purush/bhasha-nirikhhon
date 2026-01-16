// Take input string and apply obfuscate rules and output
const obfuscate = function(inputString) {
  let outputString = "";
  let currentLetter = "";
  const replaceableLetters = ["a", "e", "o", "l"];
  
  // Loop through each letter of the input string
  for (let i = 0; i < inputString.length;i++) {
    currentLetter = inputString.charAt(i);
    // Check whether current letter is any of the replaceable letters and apply function to replace if so
    for (let j = 0; j < replaceableLetters.length; j++) {
      currentLetter = checkLetters[replaceableLetters[j]](currentLetter);
    }
    outputString += currentLetter;
  }
  return outputString;
};

// Check if current inputted letter matches one of our rules, replace if so
const checkLetters = {
  a: function(inputLetter) { return replaceLetter(inputLetter, "a", "4") },
  e: function(inputLetter) { return replaceLetter(inputLetter, "e", "3") },
  o: function(inputLetter) { return replaceLetter(inputLetter, "o", "0") },
  l: function(inputLetter) { return replaceLetter(inputLetter, "l", "1") },
}

const replaceLetter = function(inputString, checkString, replacementString) {
  if (inputString === checkString) {return replacementString}
  return inputString;
}

console.log(obfuscate("Kendall"));
console.log(obfuscate("password"));
console.log(obfuscate("abracadabra"));
console.log(obfuscate("audaciously"));