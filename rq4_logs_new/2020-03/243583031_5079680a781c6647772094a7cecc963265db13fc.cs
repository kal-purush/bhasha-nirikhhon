using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DefaultEcs.System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GigglyLib.Systems
{
    public class ParticleRenderSys : ISystem<float>
    {
        SpriteBatch sb;
        public ParticleRenderSys(SpriteBatch spriteBatch)
        {
            sb = spriteBatch;
        }

        public bool IsEnabled { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void Dispose()
        { throw new NotImplementedException(); }

        public void Update(float state)
        {
            Particle p;
            for (ulong i = 0; i < ParticleManager.EndIndex; i++)
            {
                p = ParticleManager.Particles[i];
                var texture = Config.Textures[p.Texture];
                var origin = new Vector2(texture.Width / 2, texture.Height / 2);

                sb.Draw(
                    texture: texture,
                    position: new Vector2(p.X, p.Y),
                    sourceRectangle: null,
                    color: Color.White * (1 - p.Transparency),
                    rotation: p.Rotation,
                    origin: origin,
                    scale: p.Scale,
                    effects: SpriteEffects.None,
                    layerDepth: p.Depth
                );
            }
        }
    }
}