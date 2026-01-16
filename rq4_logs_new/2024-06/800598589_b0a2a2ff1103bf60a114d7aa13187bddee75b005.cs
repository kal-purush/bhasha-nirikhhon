
using Godot;

public partial class LogicAgent : Agent
{
	public override void _PhysicsProcess(double delta)
	{
		if (this.closestFood != null && this.AngleToClosestFood >= Mathf.DegToRad(1.0f))
		{
			this.RotateTowardsFood();
		}
		else if (this.closestFood != null && this.AngleToClosestFood < Mathf.DegToRad(1.0f))
		{
			this.MoveTowardsFood();
		}
		else // if (this.closestFood == null)
		{
			this.ScanForFood();
		}
		
		base._PhysicsProcess(delta);
	}

	private void RotateTowardsFood()
	{
		this.Accelerate(-1);
		this.Rotate(this.AngleToClosestFood >= 0 ? 1 : -1);
	}
	
	private void MoveTowardsFood()
	{
		this.Accelerate(1);
		this.Rotate(0);
	}

	private void ScanForFood()
	{
		this.Accelerate(-1);
		this.Rotate(1);
	}
}