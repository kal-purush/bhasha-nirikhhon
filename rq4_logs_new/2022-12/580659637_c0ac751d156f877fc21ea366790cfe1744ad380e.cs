using System;
using Xunit;

namespace EasyMicroservice.Utilities.Tests.Text
{
    public class TextExtensionsTest
    {
        [Theory]
        [InlineData("Hello World", "H", 1, new string[] { "ello World" })]
        [InlineData("Hello World", "e", 1, new string[] { "H", "llo World" })]
        [InlineData("Hello World", "o", 1, new string[] { "Hell", " World" })]
        [InlineData("Hello World", "o", 2, new string[] { "Hell", " W", "rld" })]
        [InlineData("Hello World", "o", 3, new string[] { "Hell", " W", "rld" })]
        [InlineData("Hello World", "o", 4, new string[] { "Hell", " W", "rld" })]
        [InlineData("Hello World", "d", 1, new string[] { "Hello Worl" })]
        [InlineData("Hello World", "l", 4, new string[] { "He", "o Wor", "d" })]
        [InlineData("Hello World", "l", 2, new string[] { "He", "o World"})]
        public void SplitCountTest(string input, string separator, int count, string[] expect)
        {
            var result = input.SplitCount(separator, count);
            Assert.Equal(expect, result);
        }
    }
}