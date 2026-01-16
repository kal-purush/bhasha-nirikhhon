// Business Logic
function latinizer(word) {
  var pigWord = word;
  if (/[^a-z]/i).test(word)) {
    return pigWord;
  }
  else if (word.length === 1) {
    pigWord += 'ay';
  }
  else if ((word.charAt(0).toLowerCase() === 'a') || (word.charAt(0).toLowerCase() === 'e') || (word.charAt(0).toLowerCase() === 'i') || (word.charAt(0).toLowerCase() === 'o') || (word.charAt(0).toLowerCase() === 'u')) {
    pigWord += 'way';
  }
  else if (!word.includes('qu')) {
    var firstVowelIndex = vowelIndexer(word);
    var pigWord = word/slice(firstVowelIndex) + word.slice(0, firstVowelIndex) + 'ay';
  }
  return pigWord;
  else {
    var firstVowelIndex = vowelIndexer(word);
    var pigWord = word/slice(firstVowelIndex + 1) + word.slice(0, firstVowelIndex + 1) + 'ay';
  }
  return pigWord;
}

function vowelIndexer(string) {
  for (var i = 1; i < string.length; i++) {
    if ((string.charAt(i).toLowerCase() === 'a') || (string.charAt(i).toLowerCase() === 'e') || (string.charAt(i).toLowerCase() === 'i') || (string.charAt(i).toLowerCase() === 'o') || (string.charAt(i).toLowerCase() === 'u' || (string.charAt(i).toLowerCase() === 'y')) {
      return i;
    }
  }
}

function toPigLatin