using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DesignPatterns.CreationalPatterns.FactoryMethod.Players;

namespace DesignPatterns.Test.TDD.CreationalPatterns.FactoryMethod
{
    [TestClass]
    public class ManagementBasketballPlayerTest
    {
        private ManagementPlayer _managementBasketballPlayer;

        public ManagementBasketballPlayerTest()
        {
            _managementBasketballPlayer = new ManagementBasketballPlayer();
        }

        [TestMethod, TestCategory("Factory Method")]
        public void Validate_Get_Instance_Of_Soccer_Player()
        {
            Player soccerPlayer = _managementBasketballPlayer.CreatePlayer();
            Assert.IsInstanceOfType(soccerPlayer, typeof(BasketballPlayer));
        }
    }
}