using System.Collections.Generic;
using UnityEngine;

public class StateTilemap : MonoBehaviour
{
    private Dictionary<Vector3Int, TileState> tiles = new Dictionary<Vector3Int, TileState>();

    // Method to set the state of a tile at a given position
    public void SetTile(Vector3Int position, TileState state)
    {
        if (tiles.ContainsKey(position))
        {
            tiles[position] = state; // Update the state if the tile already exists
        }
        else
        {
            tiles.Add(position, state); // Add a new tile with the specified state
        }
    }

    public bool HasTile(Vector3Int position, TileState requiredState)
    {
        if (tiles.TryGetValue(position, out TileState currentState))
        {
            return currentState == requiredState;
        }

        return false; // Return false if the tile does not exist
    }

    // Optional: Method to remove a tile from the map
    public void RemoveTile(Vector3Int position)
    {
        tiles.Remove(position);
    }
    public void ClearAllTiles()
    {
        tiles.Clear();
    }
}