using NUnit.Framework;

namespace CheckOddEven.Test
{
    public class Tests
    {
        private bool _expected;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void IsIntegerOdd_Even()
        {
            bool result = CheckOddEven.Program.IsIntegerOdd(4);               
            _expected = false;
            Assert.AreEqual(_expected, result, "Even number. Should be returned False");
        }

        [Test]
        public void IsIntegerOdd_Odd()
        {
            bool result = CheckOddEven.Program.IsIntegerOdd(13);
            _expected = true;
            Assert.AreEqual(_expected, result, "Odd number. Should be returned True");
        }

        [Test]
        public void IsIntegerOdd_NegativeOdd()
        {
            bool result = CheckOddEven.Program.IsIntegerOdd(-13);
            _expected = true;
            Assert.AreEqual(_expected, result, "Odd negative number. Should be returned True");
        }

        [Test]
        public void IsIntegerOdd_Zero()
        {
            bool result = CheckOddEven.Program.IsIntegerOdd(0);
            _expected = false;
            Assert.AreEqual(_expected, result, "Zero is Even number. Should be returned False");
        }
    }
}