using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CollectionOfHelpersTests.Sudoku;

namespace CollectionOfHelpers.Specialised.Sudoku
{

    public class SudokuCell : ValueLimitedPuzzleCell
    {
        public SudokuCell(int maxSize) : base(new BoundedLinearGenerator(maxSize))
        {
        }

        private class BoundedLinearGenerator : IPossibilitiesGenerator
        {
            private readonly int maxValue;
            public BoundedLinearGenerator(int maxValue)
            {
                this.maxValue = maxValue;
            }
            public IEnumerable<int> EnumeratePossibilities()
            {
                for (int i = 1; i <= maxValue; i++)
                {
                    yield return i;
                }
            }
        }
    }
}