using System;
using NUnit.Framework;
using TicTacToe;

namespace TicTacToe.Tests 
{
    [TestFixture]
    public class HardStratetyTest
    {
        [Test]
        public void CanScoreAWinningBoard()
        {
            string[] spaces = { "X", "X", "X",
                                "3", "4", "5",
                                "6", "7", "8"};
            Assert.AreEqual(10, HardStrategy.Score(spaces));
        }

        [Test]
        public void CanScoreATiedBoard()
        {
            string[] spaces = {"O", "O", "X",
                               "X", "X", "O",
                               "O", "X", "X"};
            Assert.AreEqual(0, HardStrategy.Score(spaces));
        }

    }
}