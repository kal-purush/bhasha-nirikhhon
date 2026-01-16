using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using OneRoomRPGJam.Entities;
using OneRoomRPGJam.System;

namespace OneRoomRPGJam
{
	public class Controller
	{
		CollisionEntity e;
		Player player; 
		List<CollisionEntity> entityList;
		int maxNumberOfEnemies; 

		//TODO Finish Enemy Spawner
		//Create random enemies based on a spawn interval. 
		//Ramp up spawn speed and amount of enemies allowed based on time played. 

		//TODO Create Projectile Class
		//This will be the base for all projectile type attacks (Bone throw, fireball, etc). 


		public Controller()
		{
			
		}
		public void Init()
		{
			//Only 1 player should exist. 
			player = new Player();

			/**How do you make sure that all spawned entities subscribe to the correct events? 
			 *You could make a new event that invokes when an entity is spawned, and then subscribing the entity to the 
			 *correct event.
			 *This approach is highly reliant on how you define an entity as "spawned". It would probably be best to invoke
			 *the event on instantiation. This would allow the subscriptions to finish before their render or update methods
			 *are even called. 
			 **/
		}

		public void Update(GameTime gameTime)
		{
			//TODO Check for specific collisions depending on different criteria
			//PlayerAttackState collisions should only be checked in the section that the player is in.
			//foreach enemy in player.currentSection
			//if player.currentState == attackingstate
			//check collision between entity and attack hitbox

			//Player to Enemy body collision checks should only occur if the player and the enemy are in the same section.
			//foreach enemy in player.currentSection
			//check collision between player and enemy 

			//Projectile collisions should only be checked in the section of the projectiles current location (this data will be updated per loop)
			//foreach projectile 


			//TODO Update all entities
		}
		public void Render(SpriteBatch spriteBatch)
		{
			//TODO Render all entities
		}

	}
}