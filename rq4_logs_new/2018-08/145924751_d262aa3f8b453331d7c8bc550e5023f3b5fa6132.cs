using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TileObject : MonoBehaviour {

    public Tile Tile { get; set; }

    public void OnMouseDown() {
        Debug.Log("You clicked Tile: " + Tile.Id + " - " + Tile.Number + Tile.Letter);
    }

    public void OnMouseEnter() {
        Debug.Log("You hovered over Tile: " + Tile.Id + " - " + Tile.Number + Tile.Letter);
    }
}