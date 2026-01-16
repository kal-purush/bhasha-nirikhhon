using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace Source.Graphics
{
    /// <summary>
    /// Make an emitter if you want a continous stream of particles from some location (which can move)
    /// If you just want one spew of particles, use the method World.MakeParticles. This will probably be overwritten eventually
    /// </summary>
    public class ParticleEmitter
    {
        private Random random;
        public Vector2 EmitterLocation;
        private List<Particle> particles;
        private List<Texture2D> textures;

        private float spawnTime { get; }
        private float currentTime;
        public bool Enabled;

        public ParticleEmitter(List<Texture2D> textures, Vector2 location)
        {
            EmitterLocation = location;
            this.textures = textures;
            this.particles = new List<Particle>();
            random = new Random();

            spawnTime = 1f / 20f;
            currentTime = 0;
            Enabled = true;
        }

        private Particle MakeParticle()
        {
            Texture2D texture = textures[random.Next(textures.Count)];
            Vector2 position = EmitterLocation;
            Vector2 velocity = new Vector2(1f * (float)random.NextDouble() * 2 - 1,
                1f * (float)random.NextDouble() * 2 - 1);
            float angle = 0;
            float angularVelocity = 0.1f * ((float)random.NextDouble() * 2 - 1);
            Color color = new Color((float)random.NextDouble(), (float)random.NextDouble(), (float)random.NextDouble());
            float size = (float)random.NextDouble();
            float liveTime = 0.5f + 2f * (float)random.NextDouble();

            return new Particle(texture, position, velocity, angle, angularVelocity, color, size, liveTime);
        }

        public void Update(float deltaTime)
        {
            for (int i = particles.Count - 1; i >= 0; i--)
            {
                particles[i].Update(deltaTime);
                if (particles[i].LiveTime < 0)
                {
                    particles.RemoveAt(i);
                }
            }

            if (Enabled)
            {
                currentTime -= deltaTime;
                while (currentTime < 0)
                {
                    currentTime += spawnTime;
                    particles.Add(MakeParticle());
                }
            }
        }

        public void Draw(SpriteBatch spriteBatch)
        {
            for (int index = 0; index < particles.Count; index++)
            {
                particles[index].Draw(spriteBatch);
            }
        }
    }
}