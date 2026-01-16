using System;
using BilletLib;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTest
{
    [TestClass]
    public class UnitTestforBil
    {
        [TestMethod]
        public void TestPrisEr240()
        {
            // arrange
             Bil Toyota =new Bil();

            // act
            int pris = Toyota.Pris();
            // Assert
            Assert.AreEqual(240,pris);
        }


        [TestMethod]
        public void TestNummerPladeLængdeForBil()
        {
            Bil BilMedLangnummerplade = new Bil();

            BilMedLangnummerplade.Nummerplade = "12345678";

            Assert.AreEqual(8, BilMedLangnummerplade.NummerPladeBegrænsing());
        }


        [TestMethod]
        public void TestForKøretøj()
        {
            // arrage
            Bil Toyota = new Bil();

            // act
            string køretøj = Toyota.KøretøjType();

            // assert

            Assert.AreEqual("Bil",køretøj);
        }

        [TestMethod]
        public void TestPrisBrobizzForBil()
        {
            // arrange
            Bil BilOverBro = new Bil();
            // act
            BilOverBro.BrobizzBrugt = true;
            // assert
            Assert.AreEqual(228,BilOverBro.Pris());
        }

        [TestMethod]
        public void TestPrisForBiloverBroMedBrobizz()
        {
            // arrange
            Bil BilOverBroMedBrobiz = new Bil();
            // act
            BilOverBroMedBrobiz.Øresundsbroen = true;
            BilOverBroMedBrobiz.BrobizzBrugt = true;
            // assert
            Assert.AreEqual(161, BilOverBroMedBrobiz.Pris());
        }

        [TestMethod]
        public void BilOverWeekend()
        {
            // arrange
            Bil WeekendBil = new Bil();

            // act
            WeekendBil.Dato = DayOfWeek.Sunday;
            WeekendBil.Øresundsbroen = false;

            // arrange

            Assert.AreEqual(192,(WeekendBil.WeekendRabat()));
        }

        [TestMethod]
        public void BilOverWeekendMBroblitz()
        {
            Bil WeekendBilBitz = new Bil();

            WeekendBilBitz.Dato = DayOfWeek.Saturday;
            WeekendBilBitz.BrobizzBrugt = true;
            WeekendBilBitz.Øresundsbroen = false;

            Assert.AreEqual(183, WeekendBilBitz.WeekendRabat());
        }
    }
}