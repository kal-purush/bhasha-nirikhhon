using System;
using TicTacToe;
using NUnit.Framework;
namespace GameTest
{
    [TestFixture]
    public class GameTest
    {
        [Test]
        public void GameStartsWithoutAWinner()
        {
            Game game = new Game();
            game.Start();
            Assert.IsFalse(game.Won());
        }

        [Test]
        public void GameStartsWithTwoPlayers()
        {
            Game game = new Game();
            game.Start();
            int playerCount = game.players.Length;
            Assert.AreEqual(2, playerCount);
        }
    }
}