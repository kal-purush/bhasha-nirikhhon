using System;
using NUnit.Framework;
using TicTacToe;

namespace UnitTestProject1
{
    [TestFixture]
    public class BoardEvaluatorTest
    {
        [Test]
        public void SpaceThatReturnMarkersAreNotEmpty()
        {
            string space = "X";
            bool result = BoardEvaluator.IsNotAnEmptySpace(space);
            Assert.IsTrue(result);
        }

        [Test]
        public void SpaceThatReturnsANonMarkerIsNotEmpty()
        {
            string space = "4";
            bool result = BoardEvaluator.IsNotAnEmptySpace(space);
            Assert.IsFalse(result);
        }

        [Test]
        public void ChecksToSeeIfAllSpacesAreTheSame()
        {
            string[] threeInARow = { "X", "X", "X" } ;
            bool allTheSame = BoardEvaluator.AllSpacesTheSame(threeInARow);
            Assert.IsTrue(allTheSame);

            string[] twoInARow = {"X", "X", "2"};
            bool almostAllTheSame = BoardEvaluator.AllSpacesTheSame(twoInARow);
            Assert.IsFalse(almostAllTheSame);
        } 

    }
}