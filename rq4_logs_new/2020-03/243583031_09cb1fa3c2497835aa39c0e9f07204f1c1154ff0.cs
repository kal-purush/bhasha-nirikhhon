using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GigglyLib.Systems
{
    public class ParallaxSys : AEntitySystem<float>
    {
        private World _world;
        private Entity _player;

        public ParallaxSys(World world, Entity player)
            : base(world.GetEntities().With<CParallaxBackground>().With<CSprite>().AsSet())
        {
            _world = world;
            _player = player;
        }

        protected override void Update(float state, in Entity entity)
        {
            var pos = _player.Get<CGridPosition>();
            ref var sprite = ref entity.Get<CSprite>();
            ref var parallax = ref entity.Get<CParallaxBackground>();

            if (_player.Has<CMoving>())
            {
                sprite.X +=
                    pos.Facing == Direction.WEST ? parallax.ScrollVelocity :
                    pos.Facing == Direction.EAST ? -parallax.ScrollVelocity :
                    0;

                sprite.Y +=
                    pos.Facing == Direction.NORTH ? parallax.ScrollVelocity :
                    pos.Facing == Direction.SOUTH ? -parallax.ScrollVelocity :
                    0;

                float width = sprite.Texture.Width;
                float height = sprite.Texture.Height;

                Console.WriteLine($"W:{width}, H:{height}");

                if (sprite.X < -width/2)
                    sprite.X += width;
                if (sprite.X > width/2)
                    sprite.X -= width;
                if (sprite.Y < -height/2)
                    sprite.Y += height;
                if (sprite.Y > height/2)
                    sprite.Y -= height;
            }

            base.Update(state, entity);
        }
    }
}