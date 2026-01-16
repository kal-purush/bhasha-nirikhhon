using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using SwinGameSDK;
// The ISeaGrid defines the read only interface of a Grid. This
// allows each player to see and attack their opponents grid.
public interface ISeaGrid
{
	int Width { get; }

	int Height { get; }
	// Indicates that the grid has changed.

	event EventHandler ISeaGrid.Changed;
	// Provides access to the given row/column
	// <param name="row">the row to access</param>
	// <param name="column">the column to access</param>
	// <value>what the player can see at that location</value>
	// <returns>what the player can see at that location</returns>

	TileView Item { get; }
	// Mark the indicated tile as shot.
	// <param name="row">the row of the tile</param>
	// <param name="col">the column of the tile</param>
	// <returns>the result of the attack</returns>
	AttackResult HitTile(int row, int col);
}