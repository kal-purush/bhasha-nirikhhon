using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CollectionOfHelpers.Specialised.Sudoku;
using NUnit.Framework;

namespace CollectionOfHelpersTests.Sudoku
{
    [TestFixture]
    public class SudokuSolverTests
    {
      //  [TestCase(0,0)]
      //  [TestCase(1, 0)]
        [TestCase(0, 1)]
        [TestCase(2, 2)]
        public void SudokuGrid_CheckAutoPossibilityReductionTest(int x, int y)
        {
            //arrange
            var sut = new SudokuDistributedGridCollection(9);
            int value = 1;
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                {
                    if(x==j && y ==i) continue;
                    sut.AssignValue(j, i, value++);
                }
            }

            //act
            var cell = sut[x, y];
            var possibleValues = cell.PossibleValues;

            //assert - the only remaining *possiblity* is the one value that wasn't assigned
            Assert.AreEqual(1, possibleValues.Count);
            Assert.AreEqual(value, possibleValues[0]);
            Assert.AreEqual(value, possibleValues[0]);
            Assert.False(cell.IsAssigned);
        }
    }
}