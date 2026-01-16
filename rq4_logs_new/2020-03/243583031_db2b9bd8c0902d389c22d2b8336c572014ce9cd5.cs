using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class AISys : AEntitySystem<float>
    {
        public AISys(World world)
            : base(world.GetEntities().With<CEnemy>().With<CMovable>().With<CGridPosition>().AsSet())
        { }

        protected override void Update(float state, in Entity entity)
        {
            Game1.TurnState = TurnState.Action;

            ref var pos = ref entity.Get<CGridPosition>();
            pos.Facing = Direction.WEST;
            pos.X--;
            entity.Set(new CMoving { Remaining = Config.TileSize });

            base.Update(state, entity);
        }
    }
}