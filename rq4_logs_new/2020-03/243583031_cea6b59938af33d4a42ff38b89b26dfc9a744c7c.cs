using System;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;
using Microsoft.Xna.Framework.Input;

namespace GigglyLib.Systems
{
    public class SpriteAnimSys : AEntitySystem<float>
    {
        public SpriteAnimSys(World world)
            : base(world.GetEntities().With<CSprite>().With<CSourceRectangle>().With<CSpriteAnimation>().AsSet())
        { }

        protected override void Update(float state, in Entity entity)
        {
            ref var sourceRect = ref entity.Get<CSourceRectangle>();
            ref var anim = ref entity.Get<CSpriteAnimation>();

            anim.SkipCounter = (anim.SkipCounter + 1) % (anim.SkipFrames + 1);

            if (anim.SkipCounter == 0)
            {
                anim.currentFrame = (anim.currentFrame + 1) % anim.TotalFrames;
                sourceRect.Rectangle.X = sourceRect.Rectangle.Width * anim.currentFrame;
            }

            Console.WriteLine(sourceRect.Rectangle.ToString());

            base.Update(state, entity);
        }
    }
}