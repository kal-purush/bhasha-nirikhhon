using Godot;
using System;

public partial class BackToMainButton : Button
{
	// Called when the node enters the scene tree for the first time.
	public override void _Ready()
	{
		// Get a reference to the button node
		 Button button = this;

		 // Assign the action (method) to call
		 button.Pressed += OnButtonPressed;
	}

	// Define the method that will be called when the button is pressed
	private void OnButtonPressed()
	{
		GD.Print("Button pressed!");
		var packedScene = GD.Load<PackedScene>("res://main.tscn");
		GetTree().ChangeSceneToPacked(packedScene);
	}
}