using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GigglyCore
{
    public static class ParticleManager
    {
        const int InitialPoolCapacity = 20;

        // Inclusive indexes
        public static ulong StartIndex = 0;
        public static ulong EndIndex = 0;

        public static ulong[] _isAlive = new ulong[InitialPoolCapacity];
        public static Particle[] _particles = new Particle[InitialPoolCapacity * 64];

        public static int CreateParticle(float x = 0, float y = 0, int texture = 0, float deltaRotation = 0, float velocity = 0, float scale = 1, float depth = 1, float transparency = 0, float rotation = 0)
        {
            Particle particle = new Particle
            {
                X = x,
                Y = y,
                Texture = (byte)texture,
                DeltaRotation = deltaRotation,
                Velocity = velocity,
                Scale = scale,
                Depth = depth,
                Transparency = transparency,
                Rotation = rotation,
            };

            bool noneDead = true;
            for (int i = 0; i < _isAlive.Length; i++)
                if (_isAlive[i] == ulong.MaxValue)
                {
                    noneDead = false;
                    break;
                }
            if (noneDead)
            {
                ulong[] newIsAlive = new ulong[_isAlive.Length * 2];
                _isAlive.CopyTo(newIsAlive, 0);
                _isAlive = newIsAlive;

                Particle[] newParticles = new Particle[_particles.Length * 2];
                _particles.CopyTo(newParticles, 0);
                _particles = newParticles;
            }

            for (ulong i = StartIndex; i < (ulong)_particles.Length; i++)
                if (CreateIfDead(i))
                {
                    _particles[i] = particle;
                    if (i > EndIndex)
                        EndIndex = i;
                    return (int)i;
                }

            for (ulong i = StartIndex-1; i >= 0; i--)
                if (CreateIfDead(i))
                {
                    _particles[i] = particle;
                    StartIndex = i;
                    return (int)i;
                }

            throw new Exception("Could not find any dead particles");
        }

        private static bool CreateIfDead(ulong id)
        {
            ulong idx = id >> 6;
            ulong offset = id & 63;
            ulong bit = (_isAlive[idx] >> (int)offset) & 1;
            if (bit == 0)
            {
                _isAlive[idx] = _isAlive[idx] ^ ((ulong)1 << (int)offset);
                return true;
            }
            return false;
        }

        public static void FreeParticle(ulong id)
        {
            ulong idx = id >> 6;
            ulong offset = id & 63;
            _isAlive[idx] = _isAlive[idx] & ~(ulong)(1 << (int)offset);
        }
    }

    public struct Particle
    {
        public float X;
        public float Y;
        public byte Texture;
        public float DeltaRotation;
        public float Velocity;
        public float Scale;
        public float Depth;
        public float Transparency;
        public float Rotation;
    }
}