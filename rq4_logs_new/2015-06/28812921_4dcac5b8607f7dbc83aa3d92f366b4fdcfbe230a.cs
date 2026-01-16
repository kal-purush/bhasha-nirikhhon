using System.Diagnostics;

namespace ConnectFour.Application.Solver
{
	[DebuggerDisplay("X = {X}, Y = {Y}")]
	public class GridPoint
	{
		public int X { get; private set; }
		public int Y { get; private set; }

		public GridPoint(int x, int y)
		{
			X = x;
			Y = y;
		}
	}
}