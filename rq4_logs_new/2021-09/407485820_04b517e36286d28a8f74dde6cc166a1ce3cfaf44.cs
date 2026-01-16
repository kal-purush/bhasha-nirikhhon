using System.Collections.Generic;
using UnityEngine;
using ZombieTiles.Enums;
using CoordinatorFunctions = System.Tuple<System.Action, System.Action, System.Action>;

namespace ZombieTiles.Mechanics
{
    public class RiverPointCoordinator
    {
        public uint X { get; private set; }
        public uint Y { get; private set; }

        private readonly uint _worldSize;

        private readonly Dictionary<RiverDirection, CoordinatorFunctions> _coordinators;

        public RiverPointCoordinator(uint x, uint y, uint worldSize)
        {
            X = x;
            Y = y;

            _worldSize = worldSize;

            _coordinators = new Dictionary<RiverDirection, CoordinatorFunctions>
            {
                { RiverDirection.North, new CoordinatorFunctions(MoveNorth, MoveEast, MoveWest) },
                { RiverDirection.West, new CoordinatorFunctions(MoveWest, MoveNorth, MoveSouth) },
                { RiverDirection.East, new CoordinatorFunctions(MoveEast, MoveSouth, MoveNorth) },
                { RiverDirection.South, new CoordinatorFunctions(MoveSouth, MoveWest, MoveEast) },
                { RiverDirection.NorthWest, new CoordinatorFunctions(MoveNorth, MoveWest, null) },
                { RiverDirection.NorthEast, new CoordinatorFunctions(MoveNorth, MoveEast, null) },
                { RiverDirection.SouthWest, new CoordinatorFunctions(MoveWest, MoveSouth, null) },
                { RiverDirection.SouthEast, new CoordinatorFunctions(MoveSouth, MoveEast, null) }
            };
        }

        public bool IsInsideOfWorld() => X <= _worldSize && Y <= _worldSize && X >= 0 && Y >= 0;

        public void Move(RiverDirection riverDirection)
        {
            var randomBool = Random.Range(0, 2) == 0;
            var randomBoolPlane = Random.Range(0, 2) == 0;

            var functionsTuple = _coordinators[riverDirection];

            var functionToInvoke = randomBool ? functionsTuple.Item1 :
                functionsTuple.Item3 != null ? randomBoolPlane ? functionsTuple.Item2 : functionsTuple.Item3 :
                functionsTuple.Item2;

            functionToInvoke();
        }

        private void MoveNorth() => Y++;
        private void MoveWest() => X--;
        private void MoveEast() => X++;
        private void MoveSouth() => Y--;
    }
}