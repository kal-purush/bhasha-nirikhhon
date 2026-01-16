using JetBrains.Annotations;
using UnityEngine;
using UnityEngine.Tilemaps;

public class WallObject : CellObject
{
    public Tile ObstacleTile; // Tile to be used for the wall object

    public override void Init(Vector2Int cell)
    {
        base.Init(cell);
        GameManager.Instance.BoardManager.SetCellTile(cell, ObstacleTile); // Set the tile for the wall object in the board manager
    }

    public override bool PlayerWantsToEnter()
    {
        return false; // Return false to prevent the player from entering the wall object
    }
}