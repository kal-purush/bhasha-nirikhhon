using NUnit.Framework;
using RomanApp.Core;

namespace RomanApp.UnitTests
{
    public class RomanSimpleSymbols
    {
        private readonly RomanConverter converter = new RomanConverter();

        [Test]
        public void RomanOne()
        {
            Assert.AreEqual("I", converter.Generate(1), "Invalid symbol for number 1");
        }

        [Test]
        public void RomanFive()
        {
            Assert.AreEqual("V", converter.Generate(5), "Invalid symbol for number 5");
        }

        [Test]
        public void RomanTen()
        {
            Assert.AreEqual("X", converter.Generate(10), "Invalid symbol for number 10");
        }

        [Test]
        public void RomanFifty()
        {
            Assert.AreEqual("L", converter.Generate(50), "Invalid symbol for number 50");
        }

        [Test]
        public void RomanHundred()
        {
            Assert.AreEqual("C", converter.Generate(100), "Invalid symbol for number 100");
        }

        [Test]
        public void RomanFiveHundred()
        {
            Assert.AreEqual("D", converter.Generate(500), "Invalid symbol for number 500");
        }

        [Test]
        public void RomanThousand()
        {
            Assert.AreEqual("M", converter.Generate(1000), "Invalid symbol for number 1000");
        }
    }

    public class RomanMaxRange
    {
        private readonly RomanConverter converter = new RomanConverter();

        [Test]
        public void MaxRange()
        {
            Assert.AreEqual("MMMCMXCIX", converter.Generate(3999), "Invalid symbol for number 3999");
        }
    }

    public class RomanInvalidRange
    {
        private readonly RomanConverter converter = new RomanConverter();

        [Test]
        public void InvalidMinimalRange()
        {
            Assert.AreEqual("Invalid number range (1-3999)", converter.Generate(0), "Invalid minimla range");
        }

        [Test]
        public void InvalidMaximalRange()
        {
            Assert.AreEqual("Invalid number range (1-3999)", converter.Generate(4000), "Invalid maximal range");
        }
    }

    public class RomanAdvancedSymbolCombinations
    {
        private readonly RomanConverter converter = new RomanConverter();

        [Test]
        public void AdvancedNumber1()
        {
            Assert.AreEqual("XXXIX", converter.Generate(39), "Invalid conversion of the number 39");
        }

        [Test]
        public void AdvancedNumber2()
        {
            Assert.AreEqual("CCXLVI", converter.Generate(246), "Invalid conversion of the number 246");
        }

        [Test]
        public void AdvancedNumber3()
        {
            Assert.AreEqual("DCCLXXXIX", converter.Generate(789), "Invalid conversion of the number 789");
        }

        [Test]
        public void AdvancedNumber4()
        {
            Assert.AreEqual("MMCDXXI", converter.Generate(2421), "Invalid conversion of the number 2421");
        }
    }

    public class RomanNumbersWithZeros
    {
        private readonly RomanConverter converter = new RomanConverter();

        [Test]
        public void NumberWithZero1()
        {
            Assert.AreEqual("CLX", converter.Generate(160), "Invalid conversion of the number 160");
        }

        [Test]
        public void NumberWithZero2()
        {
            Assert.AreEqual("CCVII", converter.Generate(207), "Invalid conversion of the number 207");
        }

        [Test]
        public void NumberWithZero3()
        {
            Assert.AreEqual("MIX", converter.Generate(1009), "Invalid conversion of the number 1009");
        }

        [Test]
        public void NumberWithZero4()
        {
            Assert.AreEqual("MLXVI", converter.Generate(1066), "Invalid conversion of the number 1066");
        }
    }
}