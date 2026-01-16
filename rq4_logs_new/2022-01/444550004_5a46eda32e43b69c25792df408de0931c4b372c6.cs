using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;

namespace drillGame
{
    public static class CollisionManager
    {
        public struct AABB
        {
            public Vector2 Position;
            public Vector2 HalfExtents;

            public AABB(Vector2 position, Vector2 halfExtents)
            {
                Position = position;
                HalfExtents = halfExtents;
            }
        }

        public struct Hit
        {
            public bool valid;

            public Vector2 position;
            public Vector2 overlap;
            public Vector2 normal;
            public float time;
        }

        public struct Sweep
        {
            public Hit hit;
            public Vector2 position;
            public float time;

            public static Sweep Invalid { get { Sweep sweep = new Sweep(); sweep.time = float.MaxValue; return sweep; } }
        }

        public static Sweep TryMoveAABB(AABB aabb, Vector2 velocity)
        {
            Vector2 a = aabb.Position - aabb.HalfExtents;
            Vector2 b = (aabb.Position + velocity) + aabb.HalfExtents;

            AABB range = new AABB(((b - a) / 2) + a, (b - a) / 2);

            //Query for objects in spatial hash. If you don't have a spatial hash yet, just use a list of all AABBs in the world.
            List<AABB> query = new List<AABB>();

            Sweep nearest = new Sweep();
            nearest.time = 1;
            nearest.position = aabb.Position + velocity;

            for (int i = 0; i < query.Count; i++)
            {
                AABB other = query[i];

                Sweep sweep = SweepAABBVsAABB(aabb, other, velocity);

                if (sweep.time < nearest.time)
                    nearest = sweep;
            }

            return nearest;
        }

        public static Sweep SweepAABBVsAABB(AABB a, AABB b, Vector2 aVelocity)
        {
            Sweep sweep = new Sweep();
            sweep.time = 1;

            if (Math.Abs(aVelocity.Length()) < float.Epsilon)
            {
                sweep.position = a.Position;

                sweep.hit = AABBVsAABB(a, b);

                if (sweep.hit.valid)
                    sweep.time = 0;
                else sweep.time = 1;

                return sweep;
            }

            sweep.hit = AABBVsSegment(b, a.Position, aVelocity, a.HalfExtents.X, a.HalfExtents.Y);

            if (sweep.hit.valid)
            {
                float ep = 1e-8f;
                sweep.time = Math.Clamp(sweep.hit.time - ep, 0, 1);
                sweep.position.X = a.Position.X + aVelocity.X * sweep.time;
                sweep.position.Y = a.Position.Y + aVelocity.Y * sweep.time;

                Vector2 direction = Vector2.Normalize(aVelocity);

                sweep.hit.position.X = Math.Clamp(
                  sweep.hit.position.X + direction.X * a.HalfExtents.X,
                  b.Position.X - b.HalfExtents.X, b.Position.X + b.Position.X);

                sweep.hit.position.Y = Math.Clamp(
                  sweep.hit.position.Y + direction.Y * a.HalfExtents.Y,
                  b.Position.Y - b.HalfExtents.Y, b.Position.Y + b.Position.Y);
            }
            else
            {
                sweep.position.X = a.Position.X + aVelocity.X;
                sweep.position.Y = a.Position.Y + aVelocity.Y;
                sweep.time = 1;
            }
            return sweep;
        }

        public static Hit AABBVsAABB(AABB a, AABB b)
        {
            float dx = b.Position.X - a.Position.X;
            float px = (b.HalfExtents.X + a.HalfExtents.X) - MathF.Abs(dx);
            if (px <= 0)
                return new Hit();

            float dy = b.Position.Y - a.Position.Y;
            float py = (b.HalfExtents.Y + a.HalfExtents.Y) - MathF.Abs(dy);
            if (py <= 0)
                return new Hit();

            Hit hit = new Hit();
            hit.valid = true;

            if (px < py)
            {
                int sx = MathF.Sign(dx);
                hit.overlap.X = px * sx;
                hit.normal.X = sx;
                hit.position.X = a.Position.X + (a.HalfExtents.X * sx);
                hit.position.Y = b.Position.Y;
            }
            else
            {
                int sy = MathF.Sign(dy);
                hit.overlap.Y = py * sy;
                hit.normal.Y = sy;
                hit.position.X = b.Position.X;
                hit.position.Y = a.Position.Y + (a.HalfExtents.Y * sy);
            }
            return hit;
        }

        public static Hit AABBVsSegment(AABB aabb, Vector2 point, Vector2 direction, float paddingX = 0, float paddingY = 0)
        {
            float scaleX = 1.0f / direction.X;
            float scaleY = 1.0f / direction.Y;
            float signX = MathF.Sign(scaleX);
            float signY = MathF.Sign(scaleY);
            float nearTimeX = (aabb.Position.X - signX * (aabb.HalfExtents.X + paddingX) - point.X) * scaleX;
            float nearTimeY = (aabb.Position.Y - signY * (aabb.HalfExtents.Y + paddingY) - point.Y) * scaleY;
            float farTimeX = (aabb.Position.X + signX * (aabb.HalfExtents.X + paddingX) - point.X) * scaleX;
            float farTimeY = (aabb.Position.Y + signY * (aabb.HalfExtents.Y + paddingY) - point.Y) * scaleY;

            if (float.IsNaN(nearTimeX))
                nearTimeX = scaleX;
            if (float.IsNaN(nearTimeY))
                nearTimeY = scaleY;

            if (nearTimeX > farTimeY || nearTimeY > farTimeX)
                return new Hit();

            float nearTime = nearTimeX > nearTimeY ? nearTimeX : nearTimeY;
            float farTime = farTimeX < farTimeY ? farTimeX : farTimeY;

            if ((nearTime >= 1 || nearTime < 0) || farTime <= 0)
                return new Hit();

            Hit hit = new Hit();
            hit.valid = true;
            hit.time = Math.Clamp(nearTime, 0, 1);
            if (nearTimeX > nearTimeY)
            {
                hit.normal.X = -signX;
                hit.normal.Y = 0;
            }
            else
            {
                hit.normal.X = 0;
                hit.normal.Y = -signY;
            }

            hit.overlap.X = (1.0f - hit.time) * -direction.X;
            hit.overlap.Y = (1.0f - hit.time) * -direction.Y;
            hit.position.X = point.X + direction.X * (hit.time + float.Epsilon);
            hit.position.Y = point.Y + direction.Y * (hit.time + float.Epsilon);
            return hit;
        }
    }
}