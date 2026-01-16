using Xunit;
using Xunit.Abstractions;
using LeetCodeSolutions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace LeetCodeTests
{

    public class Problem564Test
    {
        [Fact]
        [Trait("Category", "Small Palindrome")]
        public void SmallPalindrome1()
        {
            //Arrange
            string input = "99";
            string expected = "101";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Small Palindrome")]
        public void SmallPalindrome2()
        {
            //Arrange
            string input = "101";
            string expected = "99";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Large Palindrome")]
        public void LargePalindrome()
        {
            //Arrange
            string input = "1111568778651111";
            string expected = "1111568668651111";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Palindrome Tie")]
        public void PalindromeTie1()
        {
            //Arrange
            string input = "121";
            string expected = "111";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Palindrome Tie")]
        public void PalindromeTie2()
        {
            //Arrange
            string input = "66";
            string expected = "55";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Palindrome Tie")]
        public void PalindromeTie3()
        {
            //Arrange
            string input = "5";
            string expected = "4";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Above")]
        public void PalindromeAbove1()
        {
            //Arrange
            string input = "12300";
            string expected = "12321";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Above")]
        public void PalindromeAbove2()
        {
            //Arrange
            string input = "23453235101";
            string expected = "23453235432";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Above")]
        public void PalindromeAbove3()
        {
            //Arrange
            string input = "2345335101";
            string expected = "2345335432";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Below")]
        public void PalindromeBelow1()
        {
            //Arrange
            string input = "12345";
            string expected = "12321";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Below")]
        public void PalindromeBelow2()
        {
            //Arrange
            string input = "23453235500";
            string expected = "23453235432";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Target Below")]
        public void PalindromeBelow3()
        {
            //Arrange
            string input = "2345335999";
            string expected = "2345335432";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Single Digit")]
        public void SingleDigit1()
        {
            //Arrange
            string input = "5";
            string expected = "4";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Single Digit")]
        public void SingleDigit2()
        {
            //Arrange
            string input = "1";
            string expected = "0";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        [Trait("Category", "Number of Digits change")]
        public void DigitsChange()
        {
            //Arrange
            string input = "1000000000";
            string expected = "999999999";

            //Act
            string actual = Problem564.NearestPalindromic(input);

            //Assert
            Assert.Equal(expected, actual);
        }
    }
}