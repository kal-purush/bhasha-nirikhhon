using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;
using System.Collections.Generic;

namespace SpaceInvaders
{
    public class Body : Dynamic
    {
        public float m_maxVel;
        public float m_accel;
        public float m_friction;

        public Vector2 m_vel;
        public Vector2 m_dir;

        public Body(World world, Vector2 pos, Vector2 size, Texture2D tex,
                    float maxVel = 200.0f, float accel = 1000.0f, float friction = 5.0f)
            : base(world, pos, size, tex)
        {
            m_maxVel   = maxVel;
            m_accel    = accel;
            m_friction = friction;

            m_vel = Vector2.Zero;
            m_dir = Vector2.Zero;
        }

        public override void Update(GameTime gameTime)
        {
            float dt = (float)gameTime.ElapsedGameTime.TotalSeconds;

            float mag = m_dir.Length();
            if (mag > 0.0f)
                m_dir /= mag;

            m_vel += m_dir * m_accel * dt;

            mag = m_vel.Length();
            if (mag > m_maxVel)
            {
                m_vel = m_vel / mag * m_maxVel;
                mag = m_maxVel;
            }

            for (int a = 0; a <= 1; a++)
            {
                Vector2 testPos = m_pos;
                
                if (a == 0)
                    testPos.X += m_vel.X * dt;
                else
                    testPos.Y += m_vel.Y * dt;

                Vector2 myMin = testPos - m_size * 0.5f;
                Vector2 myMax = testPos + m_size * 0.5f;

                bool collides = false;

                foreach (Entity e in m_world.m_entities)
                {
                    if (CollidesWithMe(e))
                    {
                        if (((Dynamic)e).TestOverlapRect(myMin, myMax))
                        {
                            collides = true;
                            break;
                        }
                    }
                }

                if (!collides)
                {
                    m_pos = testPos;
                }
                else
                {
                    if (a == 0)
                        m_vel.X = 0.0f;
                    else
                        m_vel.Y = 0.0f;
                }
            }

            if (mag > 0.0f)
                m_vel *= (mag - mag * m_friction * dt) / mag;

            base.Update(gameTime);
        }

        public virtual bool CollidesWithMe(Entity e)
        {
            return (e != this) && (e is Dynamic);
        }
    }
}