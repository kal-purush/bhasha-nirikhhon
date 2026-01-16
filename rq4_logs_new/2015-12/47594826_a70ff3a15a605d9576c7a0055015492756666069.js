var getArray = require('./string-to-array.js');

function getNiceCount(input) {

  var count = 0;

  var inputArray = getArray(input, /\r?\n/);

  inputArray.forEach(function(element) {
    var vowels = ['a', 'e', 'i', 'o', 'u'];
    var presentVowels = 0;

    var wordLetters = element.split('');

    var previous = '';

    var hasAtLeastOneDoubleInRowLetter = false;

    wordLetters.forEach(function(letter) {
      if (letter == previous) {
        hasAtLeastOneDoubleInRowLetter = true;
      }
      if (vowels.indexOf(letter) != -1) {
        presentVowels++;
      }
      previous = letter;
    });

    var hasDisallowedSubstring = false;
    var disallowedSubstrings = ['ab', 'cd', 'pq', 'xy'];
    disallowedSubstrings.forEach(function(substring) {
      if (element.indexOf(substring) != -1) {
        hasDisallowedSubstring = true;
      }
    });

    if (presentVowels >= 3 && hasAtLeastOneDoubleInRowLetter && !hasDisallowedSubstring) {
      count++;
    }
  });

  return count;
}

function getNiceCountModified(input) {

  var count = 0;

  var inputArray = getArray(input, /\r?\n/);

  inputArray.forEach(function(element) {

    var hasRepeatedPair = false;
    var hasRepeatedLetterWithOneBetween = false;

    var wordLetters = element.split('');

    wordLetters.forEach(function(letter, index) {
      if (index < wordLetters.length - 2){
        var substring = letter + wordLetters[index + 1];
        if(element.indexOf(substring, index + 2) != -1){
          hasRepeatedPair = true;
        }
      }
      if(letter == wordLetters[index + 2]){
        hasRepeatedLetterWithOneBetween = true;
      }
    });

    if(hasRepeatedPair && hasRepeatedLetterWithOneBetween){
      count++;
    }

  });

  return count;
}

module.exports.getNiceCount = getNiceCount;
module.exports.getNiceCountModified = getNiceCountModified;