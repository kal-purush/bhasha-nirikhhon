using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class DamageHereSys : AEntityBufferedSystem<float>
    {
        World _world;

        public DamageHereSys(World world)
            : base(world.GetEntities().With<CGridPosition>().With<CDamageHere>().AsSet())
        {
            _world = world;
        }

        protected override void Update(float state, in Entity entity)
        {
            var pos = entity.Get<CGridPosition>();

            var ships = _world.GetEntities().With<CGridPosition>().With<CEnemy>().AsSet().GetEntities();
            for (int i = 0; i < ships.Length; i++)
            {
                var shipPos = ships[i].Get<CGridPosition>();
                if (shipPos.X == pos.X && shipPos.Y == pos.Y)
                    ships[i].Dispose();
            }

            entity.Dispose();
            base.Update(state, entity);
        }
    }
}