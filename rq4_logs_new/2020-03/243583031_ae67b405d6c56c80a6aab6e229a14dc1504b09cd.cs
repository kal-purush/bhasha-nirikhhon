using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class MoveStateSys : AEntitySystem<float>
    {
        public MoveStateSys(World world)
            : base(world.GetEntities().With<CMoveAction>().With<CGridPosition>().AsSet())
        { }

        protected override void Update(float state, in Entity entity)
        {
            ref var pos = ref entity.Get<CGridPosition>();
            var moving = entity.Get<CMoveAction>();

            pos.Facing =
                moving.DistX > 0 ? Direction.EAST :
                moving.DistX < 0 ? Direction.WEST :
                moving.DistY > 0 ? Direction.SOUTH :
                moving.DistY < 0 ? Direction.NORTH :
                pos.Facing;
            pos.X += moving.DistX;
            pos.Y += moving.DistY;

            entity.Set(new CMoving { DistX = moving.DistX * Config.TileSize, DistY = moving.DistY * Config.TileSize });
            entity.Remove<CMoveAction>();

            base.Update(state, entity);
        }
    }
}