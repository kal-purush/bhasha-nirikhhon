using System;
using System.Collections.Generic;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class FadeInSys : AEntityBufferedSystem<float>
    {
        public FadeInSys()
            : base(Game1.world.GetEntities().With<CFadeIn>().AsSet())
        {
        }

        protected override void Update(float state, in Entity entity)
        {
            ref var scale = ref entity.Get<CScalable>();
            ref var sprite = ref entity.Get<CSprite>();

            scale.Scale *= 0.945f;
            sprite.Transparency += 0.015f;
            sprite.Rotation += 0.1f;
            base.Update(state, entity);
        }
    }
}