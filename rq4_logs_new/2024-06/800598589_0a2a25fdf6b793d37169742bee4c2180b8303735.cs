using System;
using Godot;

public partial class Tree : Node2D
{
	[Export(PropertyHint.Range, "0,25,or_greater")] 
	public float FoodPerMinute { get; set; } = 10;

	[Export(PropertyHint.Range, "64,512,or_greater")]
	public float FoodSpawnRange { get; set; } = 256;

	private Timer spawnFoodTimer;
	private PackedScene packedFood = ResourceLoader.Load<PackedScene>("res://src/scenes/food.tscn");

	public override void _Ready()
	{
		this.ResetTimer();
	}

	public override void _PhysicsProcess(double delta)
	{
		this.spawnFoodTimer.Process(delta);
	}

	private void SpawnFood()
	{
		Node2D foodInstance = (Node2D)this.packedFood.Instantiate();
		this.AddChild(foodInstance);
		Vector2 spawnOffset = new Vector2(
			(float)new Random().NextDouble() * this.FoodSpawnRange - this.FoodSpawnRange/2.0f, 
			(float)new Random().NextDouble() * this.FoodSpawnRange - this.FoodSpawnRange/2.0f
		);
		foodInstance.GlobalPosition = this.GlobalPosition + spawnOffset;

		this.ResetTimer();
	}

	private void ResetTimer()
	{
		this.spawnFoodTimer.Activate(60.0f/this.FoodPerMinute);
	}

	public Tree()
	{
		this.spawnFoodTimer = new(this.SpawnFood);
	}
}