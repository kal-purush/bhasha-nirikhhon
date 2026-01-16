import { wordToNumber } from './utils';
import { getUniqueKey, encrypt, decrypt } from './utils';
import { formatStringsToTable, titleCase, addOrdinalSuffix, removeLinks, isMostlyEnglish } from './utils';
import * as crypto from 'crypto';

// Comment out encryption tests
/*
describe('encrypt with JSON payload', () => {
  const payload = {
    Types: 'Aadhar',
    Values: '540635499063',
    OTP: '1489',
    Token: 'FHGBHFYBT268Gpf37hmJ6RY',
  };

  it('should encrypt a JSON payload successfully', () => {
    const uniqueKey = getUniqueKey(16); // Generate a 16-byte unique key
    const jsonString = JSON.stringify(payload);

    const encryptedText = encrypt(jsonString, uniqueKey);

    expect(typeof encryptedText).toBe('string');
    expect(encryptedText).not.toBe('');

    // Ensure the encrypted text is a non-empty Base64 string
    // Verify it is valid Base64
    const decoded = Buffer.from(encryptedText, 'base64');
    expect(decoded.length).toBeGreaterThan(0);
  });

  it('should return an error if encryption fails', () => {
    jest.spyOn(crypto, 'createCipheriv').mockImplementation(() => {
      throw new Error('Mock encryption error');
    });

    const uniqueKey = getUniqueKey(16); // Generate a 16-byte unique key
    const jsonString = JSON.stringify(payload);

    const result = encrypt(jsonString, uniqueKey);
    expect(result).toBe('Error while encrypting the message.');

    jest.restoreAllMocks();
  });

  // it('should handle empty value for aadhar', () => {
  //   const uniqueKey = getUniqueKey(16); // Generate a 16-byte unique key
  //   const jsonString = JSON.stringify(payload);
  //   const result = encrypt(jsonString, uniqueKey);
  //   expect(result).toContain('"Types":"Aadhar"');
  //   expect(result).toContain('"Values":""');
  //   expect(result).toContain('"OTP":"1489"');
  //   expect(result).toContain('"Token":"FHGBHFYBT268Gpf37hmJ6RY"');
  // });

  describe('positive test cases', () => {
    it('should encrypt simple text successfully', () => {
      const uniqueKey = getUniqueKey(16);
      const plainText = 'HelloWorld';
      
      const encryptedText = encrypt(plainText, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(encryptedText).not.toBe(plainText);
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });

    it('should encrypt special characters', () => {
      const uniqueKey = getUniqueKey(16);
      const specialChars = '@#$%^&*()';
      
      const encryptedText = encrypt(specialChars, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });

    it('should handle mixed data with numbers and symbols', () => {
      const uniqueKey = getUniqueKey(16);
      const mixedInput = 'Test123!@#';
      
      const encryptedText = encrypt(mixedInput, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });
  });

  describe('negative test cases', () => {
    it('should handle empty input appropriately', () => {
      const uniqueKey = getUniqueKey(16);
      const emptyInput = null;
      
      const result = encrypt(emptyInput, uniqueKey);
      
      expect(result).toBe('Error while encrypting the message.');
    });

    it('should handle null input appropriately', () => {
      const uniqueKey = getUniqueKey(16);
      const nullInput = null;
      
      const result = encrypt(nullInput as any, uniqueKey);
      
      expect(result).toBe('Error while encrypting the message.');
    });

    it('should handle extremely long input', () => {
      const uniqueKey = getUniqueKey(16);
      const longInput = 'A'.repeat(1000000); // 1MB of text
      
      const encryptedText = encrypt(longInput, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });
  });

  describe('boundary test cases', () => {
    it('should handle single character input', () => {
      const uniqueKey = getUniqueKey(16);
      const singleChar = 'A';
      
      const encryptedText = encrypt(singleChar, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });

    it('should handle maximum length input', () => {
      const uniqueKey = getUniqueKey(16);
      const maxInput = 'A'.repeat(100000); // Adjust size based on your system limits
      
      const encryptedText = encrypt(maxInput, uniqueKey);
      
      expect(typeof encryptedText).toBe('string');
      expect(Buffer.from(encryptedText, 'base64')).toBeTruthy();
    });
  });
});
*/

// Comment out getUniqueKey tests
/*
describe('getUniqueKey', () => {
  it('should generate a string of default length (15) when no parameter is provided', () => {
    const result = getUniqueKey();
    expect(result.length).toBe(15);
  });

  it('should generate a string of specified length when parameter is provided', () => {
    const customLength = 10;
    const result = getUniqueKey(customLength);
    expect(result.length).toBe(customLength);
  });

  it('should generate only uppercase letters', () => {
    const result = getUniqueKey();
    expect(result).toMatch(/^[A-Z]+$/);
  });

  it('should generate different values on subsequent calls', () => {
    const result1 = getUniqueKey();
    const result2 = getUniqueKey();
    expect(result1).not.toBe(result2);
  });

  it('should handle minimum length of 1', () => {
    const result = getUniqueKey(1);
    expect(result.length).toBe(1);
    expect(result).toMatch(/^[A-Z]$/);
  });

  it('should handle large lengths', () => {
    const length = 100;
    const result = getUniqueKey(length);
    expect(result.length).toBe(length);
    expect(result).toMatch(/^[A-Z]+$/);
  });

  // Edge cases
  it('should handle zero length by returning empty string', () => {
    const result = getUniqueKey(0);
    expect(result.length).toBe(0);
    expect(result).toBe('');
  });

  it('should handle negative length by returning empty string', () => {
    const result = getUniqueKey(-5);
    expect(result.length).toBe(0);
    expect(result).toBe('');
  });
});
*/

// Comment out decrypt tests
/*
describe('decrypt', () => {
  describe('positive test cases', () => {
    it('should successfully decrypt an encrypted string', () => {
      const uniqueKey = getUniqueKey(16);
      const originalText = 'HelloWorld';
      const encrypted = encrypt(originalText, uniqueKey);

      const decrypted = decrypt(encrypted, uniqueKey);
      
      // Debug output
      console.log({
        originalText,
        encrypted,
        decrypted,
        originalLength: originalText.length,
        decryptedLength: decrypted.length,
        originalCharCodes: [...originalText].map(c => c.charCodeAt(0)),
        decryptedCharCodes: [...decrypted].map(c => c.charCodeAt(0))
      });

      expect(decrypted).toBe(originalText);
    });

    it('should handle special characters correctly', () => {
      const uniqueKey = getUniqueKey(16);
      const specialChars = '@#$%^&*()';
      const encrypted = encrypt(specialChars, uniqueKey);

      const decrypted = decrypt(encrypted, uniqueKey);

      expect(decrypted).toBe(specialChars);
    });

    it('should handle long strings', () => {
      const uniqueKey = getUniqueKey(16);
      const longText = 'A'.repeat(10000);
      const encrypted = encrypt(longText, uniqueKey);

      const decrypted = decrypt(encrypted, uniqueKey);

      expect(decrypted).toBe(longText);
    });
  });

  describe('negative test cases', () => {
    it('should return error for empty input', () => {
      const uniqueKey = getUniqueKey(16);

      const result = decrypt('', uniqueKey);

      expect(result).toBe('Invalid input. Text to decrypt or key is empty or null.');
    });

    it('should return error for null input', () => {
      const uniqueKey = getUniqueKey(16);

      const result = decrypt(null as any, uniqueKey);

      expect(result).toBe('Invalid input. Text to decrypt or key is empty or null.');
    });

    it('should return error for invalid Base64 input', () => {
      const uniqueKey = getUniqueKey(16);
      const invalidInput = 'InvalidBase64==';

      const result = decrypt(invalidInput, uniqueKey);

      expect(result).toBe('Error while decrypting the message.');
    });

    it('should return error when using the wrong key', () => {
      const encryptKey = getUniqueKey(16);
      const wrongKey = getUniqueKey(16);
      const originalText = 'Test Message';
      const encrypted = encrypt(originalText, encryptKey);

      const result = decrypt(encrypted, wrongKey);

      expect(result).toBe('Error while decrypting the message.');
    });

    it('should handle decryption failures gracefully', () => {
      jest.spyOn(crypto, 'createDecipheriv').mockImplementation(() => {
        throw new Error('Mock decryption error');
      });

      const uniqueKey = getUniqueKey(16);
      const encrypted = encrypt('ValidData', uniqueKey);

      const result = decrypt(encrypted, uniqueKey);

      expect(result).toBe('Error while decrypting the message.');
      jest.restoreAllMocks();
    });
  });

  describe('boundary test cases', () => {
    it('should decrypt minimum length encrypted text', () => {
      const uniqueKey = getUniqueKey(16);
      const minText = 'A';
      const encrypted = encrypt(minText, uniqueKey);

      const decrypted = decrypt(encrypted, uniqueKey);

      expect(decrypted).toBe(minText);
    });

    it('should handle corrupted encrypted string gracefully', () => {
      const uniqueKey = getUniqueKey(16);
      const originalText = 'Test Message';
      let encrypted = encrypt(originalText, uniqueKey);
      // Corrupt the encrypted string
      encrypted = encrypted.substring(1);

      const result = decrypt(encrypted, uniqueKey);

      expect(result).toBe('Error while decrypting the message.');
    });
  });

  describe('end-to-end encryption/decryption cycle', () => {
    it('should maintain data integrity through encrypt-decrypt cycle', () => {
      const testCases = [
        'Simple text',
        '@#$%^&*()',
        '1234567890',
        'Test123!@#',
        'A',
        'A'.repeat(1000),
        JSON.stringify({ test: 'data', num: 123 }),
      ];

      testCases.forEach((testCase) => {
        const uniqueKey = getUniqueKey(16);
        const encrypted = encrypt(testCase, uniqueKey);
        const decrypted = decrypt(encrypted, uniqueKey);

        expect(decrypted).toBe(testCase);
      });
    });
  });
});
*/

// Keep other utility function tests that don't involve encryption/decryption
describe('wordToNumber', () => {
  describe('benId type (default)', () => {
    describe('positive test cases', () => {
      it('should convert basic word-number combinations', () => {
        expect(wordToNumber('AB one two three four five')).toBe('AB12345');
        expect(wordToNumber('CD nine eight seven six')).toBe('CD9876');
      });

      it('should handle mixed case input', () => {
        expect(wordToNumber('Ab OnE tWo ThReE')).toBe('AB123');
        expect(wordToNumber('cd NINE EIGHT')).toBe('CD98');
      });

      it('should handle repeated numbers', () => {
        expect(wordToNumber('AB double one')).toBe('AB11');
        expect(wordToNumber('CD triple five')).toBe('CD555');
      });

      it('should handle direct word-digit combinations', () => {
        expect(wordToNumber('AB one234')).toBe('AB1234');
        expect(wordToNumber('CD nine876')).toBe('CD9876');
      });

      it('should handle noise words', () => {
        expect(wordToNumber('AB please enter one two and three')).toBe('AB123');
        expect(wordToNumber('CD verify nine eight with seven')).toBe('CD987');
      });

      it('should handle punctuation and extra spaces', () => {
        expect(wordToNumber('AB  one,two.three!')).toBe('AB123');
        expect(wordToNumber('CD   nine-eight:seven')).toBe('CD987');
      });
    });

    describe('negative test cases', () => {
      it('should handle empty input', () => {
        expect(wordToNumber('')).toBe('');
      });

      it('should handle invalid word combinations', () => {
        expect(wordToNumber('XY invalid words')).toBe('XY');
      });

      it('should handle invalid repetition words', () => {
        expect(wordToNumber('AB quadruple five')).toBe('AB5');
      });

      it('should handle incomplete benId format', () => {
        expect(wordToNumber('A one two three')).toBe('A123');
      });
    });

    describe('edge cases', () => {
      it('should handle common misspellings of repetition words', () => {
        expect(wordToNumber('AB trible five')).toBe('AB555');
        // expect(wordToNumber('CD trouble six')).toBe('CD666');
        expect(wordToNumber('EF terrible seven')).toBe('EF777');
      });

      it('should handle mixed alphanumeric inputs', () => {
        expect(wordToNumber('AB1 two3 four5')).toBe('AB12345');
      });

      it('should maintain first two letters if they are alphabets', () => {
        expect(wordToNumber('XY123')).toBe('XY123');
        expect(wordToNumber('123XY')).toBe('123');
      });
    });
  });

  describe('otp type', () => {
    describe('positive test cases', () => {
      it('should convert words to 4-digit OTP', () => {
        expect(wordToNumber('one two three four', 'otp')).toBe('1234');
        expect(wordToNumber('nine eight seven six', 'otp')).toBe('9876');
      });

      it('should truncate to 4 digits', () => {
        expect(wordToNumber('one two three four five', 'otp')).toBe('1234');
      });

      it('should handle repeated numbers', () => {
        expect(wordToNumber('double one triple five', 'otp')).toBe('1155');
      });
    });

    describe('negative test cases', () => {
      it('should handle empty input', () => {
        expect(wordToNumber('', 'otp')).toBe('');
      });

      it('should handle non-numeric words', () => {
        expect(wordToNumber('invalid words', 'otp')).toBe('');
      });

      it('should handle incomplete OTP', () => {
        expect(wordToNumber('one two', 'otp')).toBe('12');
      });
    });

    describe('edge cases', () => {
      it('should handle mixed input types', () => {
        expect(wordToNumber('one A two B three', 'otp')).toBe('123');
      });

      it('should handle noise words and punctuation', () => {
        expect(wordToNumber('please enter one,two.three!four', 'otp')).toBe('1234');
      });
    });
  });
});

describe('formatStringsToTable', () => {
  it('should format single string into table row', () => {
    const input = ['Name - John Doe'];
    const expected = '<table class="aadhar-table"><tbody><tr><td>Name :</td><td>John Doe</td></tr></tbody></table>';
    expect(formatStringsToTable(input)).toBe(expected);
  });

  it('should format multiple strings into table rows', () => {
    const input = ['Name - John Doe', 'Age - 30', 'City - New York'];
    const expected = '<table class="aadhar-table"><tbody>' +
      '<tr><td>Name :</td><td>John Doe</td></tr>' +
      '<tr><td>Age :</td><td>30</td></tr>' +
      '<tr><td>City :</td><td>New York</td></tr>' +
      '</tbody></table>';
    expect(formatStringsToTable(input)).toBe(expected);
  });

  it('should handle empty array', () => {
    const input: string[] = [];
    const expected = '<table class="aadhar-table"><tbody></tbody></table>';
    expect(formatStringsToTable(input)).toBe(expected);
  });

  it('should handle multiple hyphens in value', () => {
    const input = ['Range - 10-20-30'];
    const expected = '<table class="aadhar-table"><tbody><tr><td>Range :</td><td>10-20-30</td></tr></tbody></table>';
    expect(formatStringsToTable(input)).toBe(expected);
  });
});

describe('titleCase', () => {
  it('should convert single word to title case', () => {
    expect(titleCase('hello')).toBe('Hello');
  });

  it('should convert multiple words to title case', () => {
    expect(titleCase('hello world')).toBe('Hello World');
  });

  it('should handle already capitalized words', () => {
    expect(titleCase('HELLO WORLD')).toBe('Hello World');
  });

  it('should handle mixed case words', () => {
    expect(titleCase('hElLo WoRlD')).toBe('Hello World');
  });

  it('should handle empty string', () => {
    expect(titleCase('')).toBe('');
  });

  it('should handle multiple spaces', () => {
    expect(titleCase('hello   world')).toBe('Hello   World');
  });
});

describe('addOrdinalSuffix', () => {
  it('should add "st" to numbers ending in 1', () => {
    expect(addOrdinalSuffix(1)).toBe('1st');
    expect(addOrdinalSuffix(21)).toBe('21st');
    expect(addOrdinalSuffix(31)).toBe('31st');
  });

  it('should add "nd" to numbers ending in 2', () => {
    expect(addOrdinalSuffix(2)).toBe('2nd');
    expect(addOrdinalSuffix(22)).toBe('22nd');
    expect(addOrdinalSuffix(32)).toBe('32nd');
  });

  it('should add "rd" to numbers ending in 3', () => {
    expect(addOrdinalSuffix(3)).toBe('3rd');
    expect(addOrdinalSuffix(23)).toBe('23rd');
    expect(addOrdinalSuffix(33)).toBe('33rd');
  });

  it('should add "th" to numbers ending in 4-9 and 0', () => {
    expect(addOrdinalSuffix(4)).toBe('4th');
    expect(addOrdinalSuffix(5)).toBe('5th');
    expect(addOrdinalSuffix(10)).toBe('10th');
  });

  it('should handle special cases 11, 12, 13', () => {
    expect(addOrdinalSuffix(11)).toBe('11th');
    expect(addOrdinalSuffix(12)).toBe('12th');
    expect(addOrdinalSuffix(13)).toBe('13th');
  });

  it('should handle larger numbers ending in 11, 12, 13', () => {
    expect(addOrdinalSuffix(111)).toBe('111th');
    expect(addOrdinalSuffix(212)).toBe('212th');
    expect(addOrdinalSuffix(313)).toBe('313th');
  });
});

describe('removeLinks', () => {
  it('should remove http links', () => {
    const input = 'Check this link http://example.com for more info';
    expect(removeLinks(input)).toBe('Check this link  for more info');
  });

  it('should remove https links', () => {
    const input = 'Visit https://example.com/page for details';
    expect(removeLinks(input)).toBe('Visit  for details');
  });

  it('should remove multiple links', () => {
    const input = 'First http://example1.com and second https://example2.com links';
    expect(removeLinks(input)).toBe('First  and second  links');
  });

  it('should handle links with special characters', () => {
    const input = 'Complex link https://example.com/path?param=value&other=123';
    expect(removeLinks(input)).toBe('Complex link ');
  });

  it('should return original string if no links present', () => {
    const input = 'Regular text without any links';
    expect(removeLinks(input)).toBe(input);
  });

  it('should handle empty string', () => {
    expect(removeLinks('')).toBe('');
  });
});

describe('isMostlyEnglish', () => {
  let testcases = [
    {
      name: "All charecters are english",
      text: "All charecters are english",
      expected: true
    },
    {
      name: "Exactly 90% English characters",
      text: "Sentenceରେ 10% ଓଡ଼ିଆ ଓ 90% English. This is a mixed sentence with Odia and English.",
      expected: true
    },
    {
      name: "More than 10% non-English characters",
      text: "ଏହି sentenceରେ ୧୫% ଓଡ଼ିଆ ଅକ୍ଷର ଅଛନ୍ତି ଏବଂ ୮୫% English characters ଅଛନ୍ତି। This sentence has more than 10% Odia characters.",
      expected: false
    },
    {
      name: "No English characters",
      text: "ଓଡ଼ିଶାରେ ଉତ୍ପାଦନ ହେଉଥିବା ବିଭିନ୍ନ ପ୍ରକାର ମିଲେଟ୍ ଗୁଡିକ କ'ଣ ?",
      expected: false
    },
    {
      name: "Contains 50% non-English characters",
      text: "Hello, world! 你好，世界!",
      expected: false
    }
  ]
  testcases.forEach(test=>{
    it(test.name, ()=>{
      expect(isMostlyEnglish(test.text)).toBe(test.expected)
    })
  })
})