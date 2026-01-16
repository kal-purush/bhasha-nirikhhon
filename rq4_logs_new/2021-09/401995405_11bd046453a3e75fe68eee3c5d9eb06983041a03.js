const capitalize = (string) => {
  const firstLetter = string.slice(0, 1).toUpperCase();
  const resetCharacters = string.slice(1).toLowerCase();
  string = firstLetter + resetCharacters;
  return string;  
};

module.exports = capitalize;