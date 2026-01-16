using System;
using System.Collections.Generic;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GigglyLib.Systems
{
    public class ParticleBeamSys : AEntitySystem<float>
    {
        private World _world;
        private Texture2D _texture;

        public ParticleBeamSys(World world, Texture2D texture)
            : base(world.GetEntities().With<CParticleBeam>().AsSet())
        {
            _world = world;
            _texture = texture;
        }

        protected override void Update(float state, in Entity entity)
        {
            ref var beam = ref entity.Get<CParticleBeam>();
            if(beam.Frame < 6)
            {
                float sparsity = 3f;
                float distX = (beam.DestX - beam.SourceX) * Config.TileSize;
                float distY = (beam.DestY - beam.SourceY) * Config.TileSize;
                float distH = (float)Math.Sqrt(Math.Pow(distX, 2) + Math.Pow(distY, 2));

                float deltaX = distX * sparsity / distH;
                float deltaY = distY * sparsity / distH;

                float tracer = 0;
                while (tracer < distH / 6)
                {
                    var particle = _world.CreateEntity();

                    particle.Set(new CSprite
                    {
                        Texture = _texture,
                        Rotation = Config.Rand() * 2 * (float)Math.PI,
                        Transparency = (Config.Rand() * 0.2f) + 0.12f,
                        X = beam.SourceX * Config.TileSize + beam.X,
                        Y = beam.SourceY * Config.TileSize + beam.Y
                    });
                    particle.Set(new CScalable { Scale = (Config.Rand() * 0.25f) + 0.15f });
                    particle.Set(new CParticle { DeltaRotation = Config.Rand() * 0.05f, Velocity = Config.Rand() * 0.02f });

                    beam.X += deltaX;
                    beam.Y += deltaY;
                    tracer += sparsity;
                }
                beam.Frame++;
            }
            else
            {
                var toDispose = new List<Entity>();
                toDispose.Add(entity);
                foreach (var e in toDispose)
                    e.Dispose();
            }

            base.Update(state, entity);
        }
    }
}