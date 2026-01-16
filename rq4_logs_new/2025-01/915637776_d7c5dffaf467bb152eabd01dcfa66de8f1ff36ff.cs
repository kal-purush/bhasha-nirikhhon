using System.Threading.Tasks;

namespace Sandbox;

public sealed class ElectionsNetworkHelper : Component, Component.INetworkListener
{
	[Property]
	public GameObject PlayerPrefab { get; set; }

	[Property]
	public List<GameObject> Spawners { get; set; }

	protected override async Task OnLoad()
	{
		if ( Scene.IsEditor )
			return;

		if ( !Networking.IsActive )
		{
			LoadingScreen.Title = "Creating Lobby";
			await Task.DelayRealtimeSeconds( 0.1f );
			Networking.CreateLobby( new() );
		}
	}

	public void OnActive( Connection channel )
	{
		var cloned = PlayerPrefab.Clone();
		cloned.WorldPosition = Game.Random.FromList( Spawners ).WorldPosition;

		if ( cloned.Components.TryGet<Player>( out var player ) )
			player.SteamId = channel.SteamId;

		cloned.NetworkSpawn( channel );
	}
}