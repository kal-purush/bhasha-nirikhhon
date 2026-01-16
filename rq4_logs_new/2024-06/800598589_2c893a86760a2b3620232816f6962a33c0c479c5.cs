
public partial class TrainAgent : Agent
{
	private float thisFrameScore = 0.0f;
	
	public float Score => this.thisFrameScore;

	public AgentData Data => new(this.Id, this.Speed, this.energy, this.health, this.DistanceToClosestFood, this.AngleToClosestFood);

	public AgentData NormalizedData => this.Data.Normalize(this);

	public AgentAction Action;
	
	protected override void Eat(Food food)
	{
		base.Eat(food);
		this.thisFrameScore += Config.Get().Environment.Score.FoodEaten;
	}
	
	public override void _PhysicsProcess(double delta)
	{
		this.Act();
		base._PhysicsProcess(delta);
	}

	private void Act()
	{
		this.Accelerate(this.Action.AccelerateStrength);
		this.Rotate(this.Action.RotateStrength);
	}
}