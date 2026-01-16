using Sandbox;

public sealed class Player : Component
{
	public SkinnedModelRenderer SkinnedModelRenderer { get; private set; }

	protected override void OnStart()
	{
		if ( Components.TryGet<SkinnedModelRenderer>( out var renderer, FindMode.EverythingInSelfAndDescendants ) )
			SkinnedModelRenderer = renderer;

		ApplyClothing();
	}

	protected override void OnUpdate()
	{

	}

	internal void ApplyClothing()
	{
		if ( Network.Owner == null )
			return;
		if ( !SkinnedModelRenderer.IsValid() )
			return;

		var container = ClothingContainer.CreateFromLocalUser();
		container.Apply( SkinnedModelRenderer );
	}
}