using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class UpdateTargetAnimTypeSys : AEntitySystem<float>
    {
        public UpdateTargetAnimTypeSys(World world)
            : base(world.GetEntities().With<CTarget>().With<CTargetAnim>().AsSet())
        { }

        protected override void Update(float state, in Entity entity)
        {
            var target = entity.Get<CTarget>();
            ref var anim = ref entity.Get<CTargetAnim>();

            anim.TargetType =
                target.Source == "PLAYER" ? CTargetAnim.Type.PLAYER :
                target.Delay == 0 ? CTargetAnim.Type.DANGER :
                CTargetAnim.Type.WARNING;

            base.Update(state, entity);
        }
    }
}