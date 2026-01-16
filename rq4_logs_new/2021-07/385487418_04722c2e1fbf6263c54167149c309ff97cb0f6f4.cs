using NUnit.Framework;
using System.Collections.Generic;
using WordCount;

namespace WordCount.Test
{
    public class Tests
    {
        private string _expected;
        Dictionary<string, int> _result;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void WordCount_WordCountTextOf6Words()
        {
            _result = WordCount.Program.textAttributes("That's just a first test!");         
            Assert.AreEqual(_result["Words"], 6, "That's just a first test! :: contains 6 words");
            Assert.AreEqual(_result["Lines"], 1, "That's just a first test! :: contains 1 line");
            Assert.AreEqual(_result["Total Characters"], 25, "That's just a first test! :: contains 40 characters total");
            Assert.AreEqual(_result["Letter and Digit characters"], 19, "That's just a first test! :: contains 30 text characters");
        }
    }
}