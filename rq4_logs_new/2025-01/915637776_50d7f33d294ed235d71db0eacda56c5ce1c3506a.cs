using Sandbox.Network;
using System;
using System.Collections.Generic;
using static Sandbox.PhysicsGroupDescription;

namespace Sandbox;

public sealed class NetworkSorter : Component
{
	[Property]
	public SceneFile SceneFile { get; set; }

	protected override void OnStart()
	{
		JoinOrCreate();
	}

	internal async void JoinOrCreate()
	{
		try
		{
			var lobbies = await Networking.QueryLobbies();

			if ( lobbies == null || lobbies.Count() == 0 ) // No lobbies or invalid
			{
				Log.Info( "No lobbies found, creating one..." );
				CreateAndLoad();
			}
			else
			{
				if ( lobbies.All( x => x.IsFull ) )
				{
					Log.Info( "All lobbies full, creating one..." );
					CreateAndLoad();
				}
				else
				{
					Log.Info( "Lobby found, joining..." );
					var joined = await Networking.JoinBestLobby( Game.Ident );

					if ( !joined )
					{
						Log.Info( "Could not join, trying newest lobby..." );

						var freeLobbies = lobbies.Where( x => !x.IsFull );
						var newestLobby = freeLobbies.OrderByDescending( x => long.Parse( x.Name ) )
							.FirstOrDefault(); // Join the latest lobby opened, in case there's toxic ones

						Networking.Connect( newestLobby.LobbyId );
					}
				}
			}

			await Task.DelaySeconds( 1f );

			Log.Info( "Could not join, trying newest lobby again..." );

			lobbies = await Networking.QueryLobbies();
			var newFreeLobbies = lobbies.Where( x => !x.IsFull );
			var newNewestLobby = newFreeLobbies.OrderByDescending( x => long.Parse( x.Name ) )
				.FirstOrDefault(); // Join the latest lobby opened, in case there's toxic ones

			Networking.Connect( newNewestLobby.LobbyId );

			await Task.DelaySeconds( 1f );

			Log.Info( "No valid lobbies found still, creating lobby..." );
			CreateAndLoad();
		}
		catch ( Exception exception )
		{
			Log.Info( $"{exception} when joining, creating lobby..." );
			CreateAndLoad();
		}
	}

	internal void CreateAndLoad()
	{
		var currentTime = DateTime.UtcNow.Ticks;

		Networking.CreateLobby( new LobbyConfig()
		{
			MaxPlayers = 32,
			Privacy = LobbyPrivacy.Public,
			Name = currentTime.ToString(),
			DestroyWhenHostLeaves = false,
			AutoSwitchToBestHost = true,
			Hidden = false
		} );

		Scene.Load( SceneFile );
	}
}