
using System;

namespace Chess
{	
	public class Coordinate
    {
		public char Column { get; private set; }
		public int Row { get; private set; }

		public Coordinate() 
		{ 
		}

		public Coordinate(string coordinate)
        {
			Load(coordinate);
        }

		public void Load(string coordinate)
		{
            if (!IsValid(coordinate))
            {
				throw new ArgumentException();
            }

			Column = coordinate[0];
			Row = int.Parse(coordinate.Substring(1));
		}

		private bool IsValid(string coordinate)
        {
			if (coordinate.Length != 2)
				return false;

			var columnChar = coordinate[0];
			var rowString = coordinate.Substring(1);

			if (columnChar < Board.FirstColumnCoordinate || columnChar > Board.LastColumnCoordinate)
				return false;

			if (!int.TryParse(rowString, out int rowNumber))
				return false;

			if (rowNumber < Board.FirstRowCoordinate || rowNumber > Board.LastRowCoordinate)
				return false;
	
			return true;
        }
    }
}