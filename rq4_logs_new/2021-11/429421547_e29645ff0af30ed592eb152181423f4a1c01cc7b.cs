using System.Collections.Generic;
using System.Linq;

namespace PuzzleSolver.Core
{
    internal class SudokuField
    {
        public int? value;
        public int index;
        public List<int> potentialValues = new();

        public SudokuField(int? value = null)
        {
            this.value = value;

            if (value == null)
            {
                potentialValues = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            }
        }

        public int GetRowID()
        {
            // TODO: Magic numbers... moeten we wat voor verzinnen
            return index / 9;
        }

        public int GetColumnID()
        {
            // TODO: Magic numbers... moeten we wat voor verzinnen
            return index - (GetRowID() * 9);
        }

        public int GetBlockID()
        {
            // TODO: Magic numbers... moeten we wat voor verzinnen
            int boxRow = GetRowID() / 3;
            int boxCol = GetColumnID() / 3;
            return (boxRow * 3) + boxCol;
        }

        public SudokuField Copy()
        {
            return new SudokuField(value)
            {
                index = index,
                potentialValues = this.potentialValues.ToList()
            };
        }
    }
}