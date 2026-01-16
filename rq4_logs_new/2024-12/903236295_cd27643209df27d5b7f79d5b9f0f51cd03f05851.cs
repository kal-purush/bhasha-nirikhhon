namespace CalcConsoleApp.Tests
{
    public class MathClsUnitTests
    {
        MathCls mathCls;

        [SetUp]
        public void Setup()
        {
            mathCls = new MathCls();
        }

        [Test]
        public void AdditionPositiveTest1()
        {
            int expected = 7;
            int num1 = 3;
            int num2 = 4;

            //Assert
            Assert.That(mathCls.Add(num1, num2), Is.EqualTo(expected));
        }

        [Test]
        public void AdditionNegativeTest1()
        {
            int expected = 12;
            int num1 = 3;
            int num2 = 4;

            //Assert
            Assert.That(expected, Is.Not.EqualTo(mathCls.Add(num1, num2)));            
        }

        [Test]
        [Ignore("Ignore a Test - AdditionFuturWork")]
        public void AdditionFuturWork()
        {
            //Todo:
        }

        [Test]
        public void MultiplicationPositiveTest1()
        {
            int expected = 12;
            int num1 = 3;
            int num2 = 4;

            //Assert
            Assert.That(mathCls.Multiplication(num1, num2), Is.EqualTo(expected));
        }

        [Test]
        public void SubstractionPositiveTest1()
        {
            int expected = 10;
            int num1 = 13;
            int num2 = 3;

            //Assert
            Assert.That(mathCls.Substraction(num1, num2), Is.EqualTo(expected));
        }


        [Test]
        public void DivisionPositiveTest1()
        {
            int expected = 2;
            int num1 = 20;
            int num2 = 10;

            //Assert
            Assert.That(mathCls.Division(num1, num2), Is.EqualTo(expected));
        }
    }
}