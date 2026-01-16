using System;
using Advent2017.Utilities;

namespace Advent2017.Solutions
{
    class Day3
    {
        private int[,] _squares;
        private Location _target;
        private Location _center;
        private int _part2Answer;

        public void BuildSquares(int target, Action<Location,int> allocation, bool part2 = false)
        {
            var root = (int)Math.Ceiling(Math.Sqrt(target));
            if (root % 2 == 0) root++;
            
            _squares = new int[root, root];
            var mid = root / 2;
            var currentValue = 1;
            var direction = 0; // right, up, left, down
            
            var location = new Location {X = mid, Y = mid};
            _center = new Location {X = mid, Y = mid};
            
            _squares[location.X, location.Y] = currentValue++;
            
            var stepCount = 1;

            while (currentValue < target)
            {
                
                for (var i = 0; i < stepCount; i++)
                {
                    if (direction == 0) location.X++;
                    else if (direction == 1) location.Y--;
                    else if (direction == 2) location.X--;
                    else if (direction == 3) location.Y++;

                    allocation(location, currentValue++);
                    if (part2 && _squares[location.X, location.Y] > target)
                    {
                        _part2Answer = _squares[location.X, location.Y];
                        return;
                    }
                    if (currentValue == target) _target = location;
                    if (currentValue > target) break;
                }

                direction = (direction + 1) % 4;
                if (direction % 2 == 0) stepCount++; //after 2 moves, increment step
            } 
        }
        
        public void AssignValue(Location location, int currentValue)
        {
            _squares[location.X, location.Y] = currentValue;
        }

        public void AssignSumValue(Location location, int currentValue)
        {
            var sumValue = GetCell(location.X - 1, location.Y - 1) + GetCell(location.X, location.Y - 1) + GetCell(location.X + 1, location.Y - 1) +
                           GetCell(location.X - 1, location.Y) + GetCell(location.X + 1, location.Y) +
                           GetCell(location.X - 1, location.Y + 1) + GetCell(location.X, location.Y + 1) + GetCell(location.X + 1, location.Y + 1);

            _squares[location.X, location.Y] = sumValue;
        }

        public int GetCell(int x, int y)
        {
            if(x < 0 || x > _squares.GetUpperBound(0) || y < 0 || y > _squares.GetUpperBound(1))
            {
                return 0;
            }
            return _squares[x, y];
        }

        public int Solve(int target)
        {
            if (target == 1) return 0;
            BuildSquares(target, AssignValue);
            return Math.Abs(_center.X - _target.X) + Math.Abs(_center.Y - _target.Y);

        }
        
        public int Solve2(int target)
        {
            if (target == 1) return 0;
            BuildSquares(target, AssignSumValue, true);
            return _part2Answer;
        }
    }
}