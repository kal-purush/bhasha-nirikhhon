using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.Tilemaps;

using CoordinatorFunctions = System.Tuple<System.Action, System.Action, System.Action>;

public class WorldGenerator : MonoBehaviour
{
    [SerializeField]
    private Tilemap tilemap;

    [SerializeField]
    private GameObject ground;
    [SerializeField]
    private GameObject water;

    private void Start()
    {
        const uint worldSize = 160;

        GenerateGround(worldSize);

        foreach (RiverDirection riverDirection in (RiverDirection[])System.Enum.GetValues(typeof(RiverDirection)))
        {
            GenerateRiver(worldSize, 2 * worldSize / 3, worldSize / 2, worldSize / 2, riverDirection);
        }
    }

    private void GenerateGround(uint size)
    {
        var groundTile = ScriptableObject.CreateInstance<Tile>();
        groundTile.gameObject = ground;

        for (var i = 0; i < size; i++)
        {
            for (var j = 0; j < size; j++)
            {
                tilemap.SetTile(new Vector3Int(i, j, 0), groundTile);
            }
        }
    }

    private void GenerateRiver(uint worldSize,
        uint maxRiverLength,
        uint x = 0,
        uint y = 0,
        RiverDirection riverDirection = RiverDirection.North)
    {
        Assert.IsTrue(x < worldSize);
        Assert.IsTrue(y < worldSize);

        var pointCoordinator = new PointCoordinator(x, y, worldSize);

        var waterTile = ScriptableObject.CreateInstance<Tile>();
        waterTile.gameObject = water;

        for (var riverTileCounter = 0;
             pointCoordinator.IsInsideOfWorld() && riverTileCounter < maxRiverLength;
            riverTileCounter++)
        {
            tilemap.SetTile(new Vector3Int((int) pointCoordinator.X, (int) pointCoordinator.Y, 0), waterTile);
            GetNextRiverTileCoordinate(pointCoordinator, riverDirection);
        }
    }

    private class PointCoordinator
    {
        public uint X { get; private set; }
        public uint Y { get; private set; }

        private readonly uint _worldSize;

        public PointCoordinator(uint x, uint y, uint worldSize)
        {
            X = x;
            Y = y;
            _worldSize = worldSize;
        }

        public bool IsInsideOfWorld()
        {
            return X <= _worldSize && Y <= _worldSize && X >= 0 && Y >= 0;
        }

        public void MoveNorth() => Y++;
        public void MoveWest() => X--;
        public void MoveEast() => X++;
        public void MoveSouth() => Y--;
    }

    private static void GetNextRiverTileCoordinate(PointCoordinator pointCoordinator,
        RiverDirection riverDirection)
    {
        var randomBool = Random.Range(0, 2) == 0;
        var randomBoolPlane = Random.Range(0, 2) == 0;

        var coordinator = new Dictionary<RiverDirection, CoordinatorFunctions>
        {
            { RiverDirection.North, new CoordinatorFunctions(pointCoordinator.MoveNorth,
                pointCoordinator.MoveEast, pointCoordinator.MoveWest) },
            { RiverDirection.West, new CoordinatorFunctions(pointCoordinator.MoveWest,
                pointCoordinator.MoveNorth, pointCoordinator.MoveSouth) },
            { RiverDirection.East, new CoordinatorFunctions(pointCoordinator.MoveEast,
                pointCoordinator.MoveSouth, pointCoordinator.MoveNorth) },
            { RiverDirection.South, new CoordinatorFunctions(pointCoordinator.MoveSouth,
                pointCoordinator.MoveWest, pointCoordinator.MoveEast) },
            { RiverDirection.NorthWest, new CoordinatorFunctions(pointCoordinator.MoveNorth,
                pointCoordinator.MoveWest, null) },
            { RiverDirection.NorthEast, new CoordinatorFunctions(pointCoordinator.MoveNorth,
                pointCoordinator.MoveEast, null) },
            { RiverDirection.SouthWest, new CoordinatorFunctions(pointCoordinator.MoveWest,
                pointCoordinator.MoveSouth, null) },
            { RiverDirection.SouthEast, new CoordinatorFunctions(pointCoordinator.MoveSouth,
                pointCoordinator.MoveEast, null) }
        };

        var functionsTuple = coordinator[riverDirection];

        var functionToInvoke = randomBool ? functionsTuple.Item1 :
            functionsTuple.Item3 != null ? randomBoolPlane ? functionsTuple.Item2 : functionsTuple.Item3 :
            functionsTuple.Item2;

        functionToInvoke();
    }
}