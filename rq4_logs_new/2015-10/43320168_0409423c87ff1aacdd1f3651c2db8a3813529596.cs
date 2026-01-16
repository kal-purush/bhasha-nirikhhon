using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;


namespace AsteroidAssault
{
    /// <summary>
    /// This is a game component that implements IUpdateable.
    /// </summary>
   class CollisionManager 
       //pg. 151
    {
       private AsteroidManager asteroidManager;
       private PlayerManager playerManager;
       private EnemyManager enemyManager;
       private ExplosionManager explosionManager;
       private Vector2 offScreen = new Vector2(-500, -500);
       private Vector2 shotToAsteroidImpact = new Vector2(0, -20);
       private int enemyPointValue = 100;
       //pg. 151 constructor

       public CollisionManager(
           AsteroidManager asteroidManager,
           PlayerManager playerManager,
           EnemyManager enemyManager,
           ExplosionManager explosionManager)
       {
           this.asteroidManager = asteroidManager;
           this.playerManager = playerManager;
           this.enemyManager = enemyManager;
           this.explosionManager = explosionManager;
       }
       private void checkShotToEnemyCollisions()
       {
           foreach (Sprite shot in playerManager.PlayerShotManager.Shots)
           {
               foreach (Enemy enemy in enemyManager.Enemies)
               {
                   if (shot.IsCircleColliding(
                       enemy.EnemySprite.Center,
                       enemy.EnemySprite.CollisionRadius))
                   {
                       shot.Location = offScreen;
                       enemy.Destroyed = true;
                       playerManager.PlayerScore += enemyPointValue;
                       explosionManager.AddExplosion(
                           enemy.EnemySprite.Center,
                           enemy.EnemySprite.Velocity / 10);
                   }
               }
           }
       }
       //pg. 153 
       private void checkShotToAsteroidCollisions()
       {
           foreach (Sprite shot in playerManager.PlayerShotManager.Shots)
           {
               foreach (Sprite asteroid in asteroidManager.Asteroids)
               {
                   if (shot.IsCircleColliding(
                       asteroid.Center,
                       asteroid.CollisionRadius))
                   {
                       shot.Location = offScreen;
                       asteroid.Velocity += shotToAsteroidImpact;
                   }
               }
           }
       }
       //pg. 154
       private void checkShotToPlayerCollisions()
       {
           foreach (Sprite shot in enemyManager.EnemyShotManager.Shots)
           {
               if (shot.IsCircleColliding(
                   playerManager.playerSprite.Center,
                   playerManager.playerSprite.CollisionRadius))
               {
                   shot.Location = offScreen;
                   playerManager.Destroyed = true;
                   explosionManager.AddExplosion(
                       playerManager.playerSprite.Center,
                       Vector2.Zero);
               }
           }
       }
       //pg. 154- 155
       private void checkEnemyToPlayerCollisions()
       {
           foreach (Enemy enemy in enemyManager.Enemies)
           {
               if (enemy.EnemySprite.IsCircleColliding(
                   playerManager.playerSprite.Center,
                   playerManager.playerSprite.CollisionRadius))
               {
                   enemy.Destroyed = true;
                   explosionManager.AddExplosion(
                       enemy.EnemySprite.Center,
                       enemy.EnemySprite.Velocity / 10);

                   playerManager.Destroyed = true;

                   explosionManager.AddExplosion(
                       playerManager.playerSprite.Center,
                       Vector2.Zero);
               }
           }
       }
       //pg. 155
       private void checkAsteroidToPlayerCollisions()
       {
           foreach (Sprite asteroid in asteroidManager.Asteroids)
           {
               if (asteroid.IsCircleColliding(
                   playerManager.playerSprite.Center,
                   playerManager.playerSprite.CollisionRadius))
               {
                   explosionManager.AddExplosion(
                       asteroid.Center,
                       asteroid.Velocity / 10);

                   asteroid.Location = offScreen;

                   playerManager.Destroyed = true;
                   explosionManager.AddExplosion(
                       playerManager.playerSprite.Center,
                       Vector2.Zero);
               }
           }
       }
       //pg. 156
       public void CheckCollisions()
       {
           checkShotToEnemyCollisions();
           checkShotToAsteroidCollisions();
           if (!playerManager.Destroyed)
           {
               checkShotToPlayerCollisions();
               checkEnemyToPlayerCollisions();
               checkAsteroidToPlayerCollisions();
           }
       }

    }
}