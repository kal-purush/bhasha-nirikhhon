using NUnit.Framework;
using RomanNumerals;

namespace RomanNumeralsTests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void ToRomanNumeral_3_ReturnsIII()
        {
            var romanNumeral = new RomanNumeral();

            var num = 3;

            var result = romanNumeral.ToRomanNumeral(num);
            
            Assert.AreEqual("III", result);

        }
        
        [Test]
        public void ToRomanNumeral_34_ReturnsXXXIV()
        {
            var romanNumeral = new RomanNumeral();

            var num = 34;

            var result = romanNumeral.ToRomanNumeral(num);
            
            Assert.AreEqual("XXXIV", result);

        }
        
        [Test]
        public void ToRomanNumeral_86_ReturnsLXXXVI()
        {
            var romanNumeral = new RomanNumeral();

            var num = 86;

            var result = romanNumeral.ToRomanNumeral(num);
            
            Assert.AreEqual("LXXXVI", result);
        }
        
        [Test]
        public void ToRomanNumeral_2019_ReturnsMMXIX()
        {
            var romanNumeral = new RomanNumeral();

            var num = 2019;

            var result = romanNumeral.ToRomanNumeral(num);
            
            Assert.AreEqual("MMXIX", result);
        }
    }
}


// # Roman Numeral Kata
//
// Create a conversion mechanism from integers to a Roman representation (a string):
//
// * 1, 2 and 3 become I, II and III respectively.
// * 5 and 10 become V and X respectively.
// * 6 become VI, as symbols are additive.
// * 4 becomes IV
//
//
// | Symbol        | Value         |
// | ------------- |:-------------:|
// | I             | 1             |
// | V             | 5             |
// | X             | 10            |
// | L             | 50            |
// | C             | 100           |
// | D             | 500           |
// | M             | 1000          |
//
// | Numbers       |               |               |               |               |
// | :------------ |--------------:|--------------:|--------------:|--------------:|
// | 0             |               |               |               |               |
// | 1             | M             | C             | X             | I             |
// | 2             | MM            | CC            | XX            | II            |
// | 3             | MMM           | CCC           | XXX           | III           |
// | 4             |               | CD            | XL            | IV            |
// | 5             |               | D             | L             | V             |
// | 6             |               | DC            | LX            | VI            |
// | 7             |               | DCC           | LXX           | VII           |
// | 8             |               | DCCC          | LXXX          | VIII          |
// | 9             |               | CM            | XC            | IX            |
//
//
//
// Examples: 
// ```
// 34 = XXXIV = 10 + 10 + 10 + 5 - 1
// 86 = LXXXVI = 50 + 10 + 10 + 10 + 5 + 1
// ```