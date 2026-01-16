using System;
using System.Collections.Generic;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;

namespace GigglyLib.Systems
{
    public class PowerUpAnimSys : AEntityBufferedSystem<float>
    {
        public PowerUpAnimSys()
            : base(Game1.world.GetEntities().With<CPowerUp>().AsSet())
        {
        }

        protected override void Update(float state, in Entity entity)
        {
            if(entity.Get<CPowerUp>().Animate)
            {
                var toDispose = new List<Entity>();
                ref var sprite = ref entity.Get<CSprite>();
                ref var particles = ref entity.Get<CParticleSpawner>();
                ref var scale = ref entity.Get<CScalable>();
                particles.Texture = "particles-rainbow";
                particles.Impact += 1.2f;
                sprite.Transparency += 0.07f;
                scale.Scale += 0.4f;
                if (sprite.Transparency > 1)
                    toDispose.Add(entity);
                foreach (var e in toDispose)
                    e.Dispose();
            }

            base.Update(state, entity);
        }
    }
}