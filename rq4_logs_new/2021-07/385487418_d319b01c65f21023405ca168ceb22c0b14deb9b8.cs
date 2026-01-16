using NUnit.Framework;
using Exercise1;
using System.IO;
using System;

namespace Exercise1_Test
{
    public class Tests
    {
        private bool _expected;

        [SetUp]
        public void Setup()
        {           
        }

        [Test]
        public void ReturnFifteens_BothNumbersEqual15()
        {
            bool result = Exercise1.Program.ReturnFifteens(15, 15);
            _expected = true;
            Assert.AreEqual(_expected, result, "Both of numbers are 15. Should be returned True");
        }

        [Test]
        public void ReturnFifteens_OneNumberEquals15()
        {
            bool result = Exercise1.Program.ReturnFifteens(2, 15);
            _expected = true;
            Assert.AreEqual(_expected, result, "One number is 15. Should be returned True");
        }

        [Test]
        public void ReturnFifteens_SumEquals15()
        {
            bool result = Exercise1.Program.ReturnFifteens(7, 8);
            _expected = true;
            Assert.AreEqual(_expected, result, "Sum is 15. Should be returned True");
        }

        [Test]
        public void ReturnFifteens_DiffEquals15()
        {
            bool result = Exercise1.Program.ReturnFifteens(22, 7);
            _expected = true;
            Assert.AreEqual(_expected, result, "Difference is 15. Should be returned True");
        }

        [Test]
        public void ReturnFifteens_Not15()
        {
            bool result = Exercise1.Program.ReturnFifteens(4, 3);
            _expected = false;
            Assert.AreEqual(_expected, result, "No of the actions equal 15. Should be returned False");
        }
    }
}