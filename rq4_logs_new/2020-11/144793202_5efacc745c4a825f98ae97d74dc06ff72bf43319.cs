using System;
using System.Collections.Generic;
using MazeWalker.Domain.Location;

namespace MazeWalker.Domain.Walker
{
    public class Hand : Attribute
    {
        RotateDirection HandDirection;
        Dictionary<Direction, Direction> ClockWiseDirection;
        Dictionary<Direction, Direction> CounterClockWiseDirection;

        public Hand(RotateDirection handDirection)
        {
            HandDirection = handDirection;

            ClockWiseDirection = new Dictionary<Direction, Direction>();
            ClockWiseDirection.Add(Direction.North, Direction.East);
            ClockWiseDirection.Add(Direction.East, Direction.South);
            ClockWiseDirection.Add(Direction.South, Direction.West);
            ClockWiseDirection.Add(Direction.West, Direction.North);

            CounterClockWiseDirection = new Dictionary<Direction, Direction>();
            CounterClockWiseDirection.Add(Direction.North, Direction.West);
            CounterClockWiseDirection.Add(Direction.West, Direction.South);
            CounterClockWiseDirection.Add(Direction.South, Direction.East);
            CounterClockWiseDirection.Add(Direction.East, Direction.North);
        }

        public Direction GetDirection(Direction faceDirection)
        {
            switch(HandDirection)
            {
                case RotateDirection.Clockwise:
                    return ClockWiseDirection[faceDirection];
                case RotateDirection.CounterClockwise:
                    return CounterClockWiseDirection[faceDirection];
                default:
                    throw new NotImplementedException();
            }
        }

        public Direction GetCounterDirection(Direction faceDirection)
        {
            switch (HandDirection)
            {
                case RotateDirection.Clockwise:
                    return CounterClockWiseDirection[faceDirection];
                case RotateDirection.CounterClockwise:
                    return ClockWiseDirection[faceDirection];
                default:
                    throw new NotImplementedException();
            }
        }
    }
}