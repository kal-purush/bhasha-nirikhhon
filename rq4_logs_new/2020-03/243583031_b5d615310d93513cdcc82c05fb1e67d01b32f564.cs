using System;
using System.Collections.Generic;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class PowerUpSys : AEntityBufferedSystem<float>
    {
        public PowerUpSys()
            : base(Game1.world.GetEntities().With<CPowerUp>().AsSet())
        {
        }

        protected override void Update(float state, in Entity entity)
        {
            var pos = entity.Get<CGridPosition>();
            var playerPos = Game1._player.Get<CGridPosition>();
            var toDispose = new List<Entity>();
            if (pos.X == playerPos.X && pos.Y == playerPos.Y)
            {
                var drop = entity.Get<CWeaponsArray>().Weapons[0];
                ref var playerWeapons = ref Game1._player.Get<CWeaponsArray>();
                playerWeapons.Weapons.Add(drop);
                ref var playerHealth = ref Game1._player.Get<CHealth>();
                playerHealth.Damage = 0;
                toDispose.Add(entity);
            }

            foreach (var e in toDispose)
                e.Dispose();

            base.Update(state, entity);
        }
    }
}