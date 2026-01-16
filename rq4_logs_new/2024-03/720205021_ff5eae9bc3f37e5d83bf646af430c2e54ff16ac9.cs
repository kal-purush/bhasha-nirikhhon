using Godot;
using System;

public partial class TileMapController : TileMap
{
	private const int TileWidth = 100;
	private const int TileHeight = 100;

	// Called when the node enters the scene tree for the first time.
	public override void _Ready()
	{
	}

	// Called every frame. 'delta' is the elapsed time since the previous frame.
	public override void _Process(double delta)
	{
	}

	public override void _Input(InputEvent @event)
	{
		if (@event is InputEventMouseButton eventMouseButton)
		{
			if (eventMouseButton.ButtonIndex != MouseButton.Left || eventMouseButton.IsReleased()) return;

			Vector2 globalClickPos = eventMouseButton.GlobalPosition;
			Vector2 posToLocal = ToLocal(globalClickPos);

			// Melyik hexben vagyunk 
			Vector2 localPosToMap = LocalToMap(posToLocal);

			GD.Print(localPosToMap);

			// Vector2 dest = new Vector2(TileWidth * localPosToMap.X, TileHeight * localPosToMap.Y);
		}
	}
}