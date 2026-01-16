using Microsoft.VisualStudio.TestTools.UnitTesting;
using SpaceGame;

namespace Test
{
    [TestClass]
    public class Testing
    {
        GameStateManager g = new GameStateManager(Difficulty.Hard);

        [TestMethod]
        public void GoldMax()
        {
            g.Ship.Coins += 100000;
            g.CurrentPlanet.Buy("gold", 1000, g.Ship, g.CurrentPlanet);
            Assert.AreEqual(g.Ship.Gold, 1000/3);
        }
        public void FuelMax()
        {
            g.Ship.Coins += 100000;
            g.CurrentPlanet.Buy("fuel", 1000, g.Ship, g.CurrentPlanet);
            Assert.AreEqual(g.Ship.Fuel, 1000 / 3);
        }
        public void HullMax()
        {
            g.Ship.Coins += 100000;
            g.CurrentPlanet.Buy("hull", 1000, g.Ship, g.CurrentPlanet);
            Assert.AreEqual(g.Ship.Hull, 1000 / 3);
        }
    }
}