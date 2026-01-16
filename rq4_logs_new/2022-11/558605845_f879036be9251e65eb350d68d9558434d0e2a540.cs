using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using BusinessServices;
using Domain;

namespace Test
{
    [TestClass]
    public class PlayerManagementTest
    {
        [TestMethod]
        public void GetPlayersFilteredName_Test_Success()
        {
            BusinessServices.PlayersManagement playersManagement = new BusinessServices.PlayersManagement();
            String testName = "Rob";
            List<Domain.Players> playersTest = playersManagement.GetPlayersFilteredName(testName);

            int expected = 2;
            int result = playersTest.Count;

            Assert.AreEqual(expected, result);
        }
        [TestMethod]
        public void GetFriendsPlayersOfPlayerByUser_Test_Success()
        {
            BusinessServices.PlayersManagement playersManagement = new BusinessServices.PlayersManagement();
            
            Domain.Players playerTest = new Domain.Players();
            playerTest.idPlayer = 8010;

            List<Domain.Players> playersTest = playersManagement.GetFriendsPlayersOfPlayerByUser(playerTest);


            int expected = 2;
            int result = playersTest.Count;

            Assert.AreEqual(expected, result);
        }

        [TestMethod]
        public void IsFriend_Test_Success()
        {
            BusinessServices.PlayersManagement playersManagement = new BusinessServices.PlayersManagement();

            Domain.Players playerOrigin = new Domain.Players();
            Domain.Players playerTarget = new Domain.Players();

            playerOrigin.idPlayer = 8010;
            playerTarget.idPlayer = 8080;

            bool expected = true;
            bool result = playersManagement.IsFriend(playerOrigin, playerTarget);

            Assert.AreEqual(expected, result);
        }
        //Los test de inserción solo se ejecutan al momento de la demostración para evitar conflictos
        /*
        [TestMethod]
        public void AddFriendPlayer_Test_Success()
        {
            BusinessServices.PlayersManagement playersManagement = new BusinessServices.PlayersManagement();

            Domain.Players playerOrigin = new Domain.Players();
            Domain.Players playerTarget = new Domain.Players();

            playerOrigin.idPlayer = 8080;
            playerTarget.idPlayer = 8010;

            bool expected = true;
            bool result = playersManagement.AddFriendPlayer(playerOrigin,playerTarget);


            Assert.AreEqual(expected, result);
        }*/
        
    }
}