using Sandbox;

public abstract class Actor : Component
{
	[Property]
	[Category( "Name" )]
	public virtual string FirstName { get; set; } = "John";

	[Property]
	[Category( "Name" )]
	public virtual string LastName { get; set; } = "Doe";

	[Property]
	[Category( "Name" )]
	public virtual string UserName { get; set; }

	[Property]
	[Category( "Name" )]
	public string FullName => string.IsNullOrWhiteSpace( UserName ) ? $"{FirstName} {LastName}" : UserName;

	protected override void OnUpdate()
	{

	}
}