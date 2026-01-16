using Platformer.Core;
using Platformer.Mechanics;
using Platformer.Model;
using UnityEngine;

namespace Platformer.Gameplay
{
    /// <summary>
    /// Fired when a player enters a spike hazard trigger.
    /// </summary>
    /// <typeparam name="PlayerCollision"></typeparam>

    public class PlayerEnteredSpikeHazard : Simulation.Event<PlayerEnteredSpikeHazard>
    {
        public PlayerController player;
        public SpikeHazard spikezone;
        public GameObject respawnPoint;

        PlatformerModel model = Simulation.GetModel<PlatformerModel>();

        public override void Execute()
        {
            
		   player.Teleport(respawnPoint.transform.position);
               	
        }
    }
}