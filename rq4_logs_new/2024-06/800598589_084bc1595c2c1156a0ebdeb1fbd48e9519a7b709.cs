using Godot;
using System;

public partial class Agent : CharacterBody2D
{
	[Export]
	public float MaximumSpeed { get; set; } = 500.0f;
	
	[Export]
	public float MaximumEnergy { get; set; } = 200.0f;
	
	[Export]
	public float StartEnergy { get; set; } = 50.0f;
	
	[Export]
	public float MaximumHealth { get; set; } = 200.0f;
	
	[Export]
	public float StartHealth { get; set; } = 100.0f;
	
	[Export]
	public float MaximumTurnSpeedRadiansPerSecond { get; set; } = 6.0f;
	
	[Export]
	public float SightAngleRadians { get; set; } = 2.0f;
		
	private float energy;
	private float health;
	private float rotateRadiansPerSecond = 0.0f;
	
	public float getPointingAngle()
	{
		return Velocity.Angle();
	}
	
	private void lowerEnergy(float energyLoss)
	{
		this.energy -= energyLoss;
	}
	
	private void lowerHealth(float healthLoss)
	{
		this.health -= healthLoss;
	}
	
	public override void _Ready()
	{
		this.energy = this.StartEnergy;
		this.health = this.StartHealth;
	}
	
	public override void _Process(double delta)
	{
		
	}

	public override void _PhysicsProcess(double delta)
	{
		if (this.energy != 0){
			lowerEnergy((float)(Config.Instance.Data.Environment.EnergyLossPerSecond * delta));
		}
		else {
			lowerHealth((float)(Config.Instance.Data.Environment.HealthLossPerSecond * delta));
		}
		Rotate((float) delta * rotateRadiansPerSecond);
		Velocity = new Vector2(Mathf.Cos(Rotation), Mathf.Sin(Rotation)) * Velocity.Length();
		MoveAndCollide(Velocity);
	}
}