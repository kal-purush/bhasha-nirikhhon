using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GigglyLib.Systems
{
    public class ExplosionAnimSys : AEntitySystem<float>
    {
        private World _world;
        private Texture2D _explosionTexture;
        private Texture2D _particleTexture;

        public ExplosionAnimSys(World world, Texture2D explosionTexture, Texture2D particleTexture)
            : base(world.GetEntities().With<CExplosionAnim>().With<CGridPosition>().AsSet())
        {
            _world = world;
            _explosionTexture = explosionTexture;
            _particleTexture = particleTexture;
        }

        protected override void Update(float state, in Entity entity)
        {
            var pos = entity.Get<CGridPosition>();
            if (!entity.Has<CSprite>())
            {
                entity.Set(new CParticleSpawner {
                    Texture = _particleTexture,
                    Impact = 3
                });
                entity.Set(new CSprite
                {
                    X = pos.X * Config.TileSize,
                    Y = pos.Y * Config.TileSize,
                    Texture = _explosionTexture,
                    Rotation = Config.Rand() * (float) Math.PI * 2,
                    Depth = 0.7f
                });
                entity.Set(new CScalable
                {
                    Scale = 0.1f
                });
            }
            else
            {
                ref var sprite = ref entity.Get<CSprite>();
                ref var scale = ref entity.Get<CScalable>();
                sprite.Transparency += 0.3f;
                scale.Scale *= 2f;
                if (sprite.Transparency > 1.0f)
                {
                    entity.Remove<CSprite>();
                    entity.Remove<CExplosionAnim>();
                }
            }

            base.Update(state, entity);
        }
    }
}