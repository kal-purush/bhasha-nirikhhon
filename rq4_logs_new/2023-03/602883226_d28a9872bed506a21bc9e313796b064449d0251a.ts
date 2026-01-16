function solution(inputString: string): boolean {
  interface objectAlphabet {
    [key: string]: number;
  }
  const sortedArrayValue: number[] = [];
  const sortedArrayLetter: string[] = [];
  const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('');
  const letterOccurances: objectAlphabet = {};

  for (let i = 0; i < inputString.length; i++) {
    if (!letterOccurances[inputString[i]]) {
      letterOccurances[inputString[i]] = 1;
    } else {
      letterOccurances[inputString[i]]++;
    }
  }

  Object.keys(letterOccurances)
    .sort()
    .forEach(v => {
      sortedArrayValue.push(letterOccurances[v]);
      sortedArrayLetter.push(v);
    });
  const lastLetterInString: string = sortedArrayLetter[sortedArrayLetter.length - 1];
  const slicedAlphabet: string[] = alphabet.slice(0, alphabet.indexOf(lastLetterInString) + 1);
  if (slicedAlphabet.length > sortedArrayLetter.length) {
    return false;
  }
  for (let j = 0; j < sortedArrayValue.length; j++) {
    if (sortedArrayValue[j] < sortedArrayValue[j + 1]) {
      return false;
    }
  }
  return true;
}

solution('bbbcdafe');