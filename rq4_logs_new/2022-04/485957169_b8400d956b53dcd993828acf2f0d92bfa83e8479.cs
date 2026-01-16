using NUnit.Framework;
using Roleplay;

namespace Test.Library
{
    public class BreastplateTest
    {
        // Testea el metodo AttackValue si attackValue es positivo.
        [Test]
        public void attackValueTest1()
        {
            Breastplate breastplate = new Breastplate(20,0);
            int expected = 20;
            int actual = breastplate.AttackValue;
            Assert.AreEqual(expected,actual);
        }

        // Testea el metodo AttackValue si attackValue es negativo.
        [Test]
        public void attackValueTest2()
        {
            Breastplate breastplate = new Breastplate(-20,0);
            int expected = 0;
            int actual = breastplate.AttackValue;
            Assert.AreEqual(expected,actual);
        }

        // Testea el metodo DefenseValue si defenseValue es positivo.
        [Test]
        public void defenseValueTest1()
        {
            Breastplate breastplate = new Breastplate(0,20);
            int expected = 20;
            int actual = breastplate.DefenseValue;
            Assert.AreEqual(expected,actual);
        }

        // Testea el metodo DefenseValue si defenseValue es negativo.
        [Test]
        public void defenseValueTest2()
        {
            Breastplate breastplate = new Breastplate(0,-20);
            int expected = 0;
            int actual = breastplate.DefenseValue;
            Assert.AreEqual(expected,actual);
        }
    }
}