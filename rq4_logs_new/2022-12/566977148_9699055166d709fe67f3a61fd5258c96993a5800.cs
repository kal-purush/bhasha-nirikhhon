using NUnit.Framework;
using CardCodedDate;


namespace ProgrammTests
{
    public class Tests
    { 
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void DateCoder_Test2()
        {
            DateCalc.Inic();
            int a = 5;
            int b = 2;
            string c = DateCalc.DateCoder(a, b);
            Assert.IsTrue("Это: Телец ♠ 9" == c);
        }

        [Test]
        public void DateCoder_Test1()
        {
            DateCalc.Inic();
            int a = 11;
            int b = 3;
            string c = DateCalc.DateCoder(a,b);
            Assert.IsTrue("Это: Скорпион ♠ 8" == c);
        }
    }
}