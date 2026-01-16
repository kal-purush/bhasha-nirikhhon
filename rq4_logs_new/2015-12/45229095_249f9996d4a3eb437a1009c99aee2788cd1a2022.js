import secureRandom from 'secure-random';
import {assign, includes} from 'lodash';

const dw = {
  en: require('diceware-wordlist-en'),
  swe: require('diceware-wordlist-swe'),
  jp: require('diceware-wordlist-jp'),
  sp: require('diceware-wordlist-sp')
};

const getRandomInt = (min, max) => {
  // Create byte array and fill with 1 random number
  const byteArray = secureRandom(1, {type: 'Uint8Array'});
  const range = max - min + 1;
  const maxRange = 256;
  if (byteArray[0] >= Math.floor(maxRange / range) * range)
    return getRandomInt(min, max);
  return min + (byteArray[0] % range);
};

const diceRoll = () => {
  return getRandomInt(1, 6);
};

const diceSeq = (count) => {
  const seq = [];
  for(let i=0; i<count; i++) {
    seq.push(diceRoll());
  }
  return seq.join('');
};

const getDices = () => {
  return diceSeq(5);
};

const getRandomWord = (language) => {
  return dw[language][getDices()];
};

const getRandomPassword = (options) => {
  options = assign({
    'language': 'en',
    'wordcount': 6,
    'format': 'string'
  }, options);
  if (!includes(['en', 'swe', 'jp', 'sp'], options.language)) {
    throw(new Error(`Unsupported language: ${options.language}`));
  }
  const words = [];
  for (var i=0; i<options.wordcount; i++) {
    words.push(getRandomWord(options.language));
  }
  if (options.format === 'array') {
    return words;
  } else {
    return words.join(' ');
  }
};

export default getRandomPassword;