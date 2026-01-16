const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

function newIndex(char, shift) {
  let index = LETTERS.indexOf(char.toUpperCase());
  index += shift;
  if (index > 25) index -= 26;
  return index;
}

function caesarEncrypt(string, shift) {
  return string.split('').map((char) => {
    if (char.match(/[a-z]/i)) {
      const index = newIndex(char, shift);
      if (char.match(/[A-Z]/)) return LETTERS[index].toUpperCase();
      if (char.match(/[a-z]/)) return LETTERS[index].toLowerCase();
    }
    return char;
  }).join('');
}

// simple shift
console.log(caesarEncrypt('A', 0));       // "A"
console.log(caesarEncrypt('A', 3));       // "D"

// wrap around
console.log(caesarEncrypt('y', 5));       // "d"
console.log(caesarEncrypt('a', 47));      // "v"

// all letters
console.log(caesarEncrypt('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 25));
// "ZABCDEFGHIJKLMNOPQRSTUVWXY"
console.log(caesarEncrypt('The quick brown fox jumps over the lazy dog!', 5));
// "Ymj vznhp gwtbs ktc ozrux tajw ymj qfed itl!"

// many non-letters
console.log(caesarEncrypt('There are, as you can see, many punctuations. Right?); Wrong?', 2));
// "Vjgtg ctg, cu aqw ecp ugg, ocpa rwpevwcvkqpu. Tkijv?); Ytqpi?"