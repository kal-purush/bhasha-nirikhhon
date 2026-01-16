using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Monopoly_Model;

namespace Monopoly_UnitTests
{
    [TestClass]
    public class SettingsTest
    {
        [TestMethod]
        public void Bedrag_ValueIsGroterDanHonderd_BedragIsGelijkAanValue()
        {
            //arrange
            Settings settings = new Settings();

            //act
            settings.Bedrag = 2000;

            //assert
            Assert.AreEqual(2000, settings.Bedrag);

        }

        public void Bedrag_ValueIsKleinerDanHonderd_BedragIsGelijkAanTweeDuizend()
        {
            //arrange
            Settings settings = new Settings();

            //act
            settings.Bedrag = 50;

            //assert
            Assert.AreEqual(2000, settings.Bedrag);
        }

        public void Spelers_ValueIsKleinerDanTwee_SpelersIsGelijkAanTwee()
        {
            //arrange
            Settings settings = new Settings();

            //act
            settings.Spelers = 1;

            //assert
            Assert.AreEqual(2, settings.Spelers);
        }

        public void Spelers_ValueIsGroterDanZes_SpelersIsGelijkAanTwee()
        {
            //arrange
            Settings settings = new Settings();

            //act
            settings.Spelers = 10;

            //assert
            Assert.AreEqual(2, settings.Spelers);
        }

        public void Spelers_ValueIsGeldig_SpelersIsGelijkAanValue()
        {
            //arrange
            Settings settings = new Settings();

            //act
            settings.Spelers = 4;

            //assert
            Assert.AreEqual(4, settings.Spelers);
        }
    }
}