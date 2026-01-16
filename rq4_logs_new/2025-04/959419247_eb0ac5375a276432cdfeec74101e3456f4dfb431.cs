using UnityEngine;
using UnityEngine.Tilemaps;

public class ExitCellObject : CellObject
{
    public Tile endTile;

    public override void Init(Vector2Int coord)
    {
        base.Init(coord); // Call the base class Init method
        GameManager.Instance.BoardManager.SetCellTile(coord, endTile); // Set the tile for the cell
    }

    public override void PlayerEntered()
    {
        Debug.Log("Player has entered the exit cell!"); // Log when the player enters the exit cell
        GameManager.Instance.NewLevel(); // Call the NewLevel method in GameManager
    }
}