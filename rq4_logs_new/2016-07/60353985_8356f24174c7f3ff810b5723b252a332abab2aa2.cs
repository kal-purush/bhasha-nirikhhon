namespace GameOne.Source.World
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Entities;
    using Enumerations;
    using Events;
    using Factories;

    public class EntityHandler
    {
        private List<Entity> register;

        public EntityHandler()
        {
            register = new List<Entity>();
        }

        public void ProcessEntities(List<Entity> entities, List<Tile> tiles, double time)
        {
            foreach (Entity entity in entities)
            {
                entity.Update(time);
            }

            Physics.CollisionResolution(entities
                    .OfType<Model>()
                    .Where(e => e.Alive)
                    .ToList());
            Physics.BoundsCheck(entities
                    .OfType<Model>()
                    .ToList(),
                    tiles
                    .Where(tile => tile.TileType == TileType.Wall)
                    .ToList());

            // Add new entities to list
            foreach (var item in register)
            {
                entities.Add(item);
            }
            register.Clear();

            // Remove dead entities
            RemoveDead(entities);
        }

        private void RemoveDead(List<Entity> entities)
        {
            List<Entity> result = entities.Where(e => e is Model && !((Model)e).Alive).ToList();
            foreach (var item in result)
            {
                entities.Remove(item);
            }
        }

        public void Subscribe(List<Entity> entities)
        {
            foreach (var entity in entities)
            {
                if (entity is Character && !(entity is Player))
                {
                    ((Character)entity).FireProjectileEvent += RegisterProjectile;
                }
            }
        }

        public void SubscribeToPlayer(Player player)
        {
            player.FireProjectileEvent += RegisterProjectile;
        }

        private void RegisterProjectile(object sender, ProjectileEventArgs e)
        {
            Projectile projectile = ProjectileFactory.MakeProjectile((Character)sender, e.Type);
            register.Add(projectile);
        }
    }
}