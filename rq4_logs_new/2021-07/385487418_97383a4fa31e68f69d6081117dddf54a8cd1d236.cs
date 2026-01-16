using NUnit.Framework;

namespace CozaLozaWoza.Test
{
    public class Tests
    {
        private string _expected;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void CozaLozaWoza_Coza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(9);
            _expected = "Coza";
            Assert.AreEqual(_expected, result, "Coza should be returned for 9");
        }

        [Test]
        public void CozaLozaWoza_Loza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(20);
            _expected = "Loza";
            Assert.AreEqual(_expected, result, "Loza should be returned for 20");
        }

        [Test]
        public void CozaLozaWoza_Woza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(14);
            _expected = "Woza";
            Assert.AreEqual(_expected, result, "Woza should be returned for 14");
        }

        [Test]
        public void CozaLozaWoza_CozaLoza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(15);
            _expected = "CozaLoza";
            Assert.AreEqual(_expected, result, "CozaLoza should be returned for 15");
        }

        [Test]
        public void CozaLozaWoza_CozaWoza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(21);
            _expected = "CozaWoza";
            Assert.AreEqual(_expected, result, "CozaWoza should be returned for 21");
        }

        [Test]
        public void CozaLozaWoza_LozaWoza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(35);
            _expected = "LozaWoza";
            Assert.AreEqual(_expected, result, "LozaWoza should be returned for 35");
        }

        [Test]
        public void CozaLozaWoza_CozaLozaWoza()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(105);
            _expected = "CozaLozaWoza";
            Assert.AreEqual(_expected, result, "CozaLozaWoza should be returned for 105");
        }

        [Test]
        public void CozaLozaWoza_Number()
        {
            string result = CozaLozaWoza.Program.CozaLozaWoza(41);
            _expected = "41";
            Assert.AreEqual(_expected, result, "41 should be returned for 41");
        }
    }
}