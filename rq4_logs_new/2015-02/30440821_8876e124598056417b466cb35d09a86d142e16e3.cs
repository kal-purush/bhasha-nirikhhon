using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace RkoOuttaNowhere.Gameplay.Units
{
    public class UnitFactory
    {
        public static const int WEAK_HEALTH     = 20;
        public static const int MEDIUM_HEALTH   = 50;
        public static const int STRONG_HEALTH   = 100;
        public static const float SLOW_SPEED     = 60;
        public static const float MEDIUM_SPEED   = 80;
        public static const float FAST_SPEED     = 100;

        public static Unit CreateWeakMelee()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, MEDIUM_SPEED, WEAK_HEALTH, Behaviour.BasicMove);
            return u;
        }

        public static Unit CreateMediumMelee()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, MEDIUM_SPEED, MEDIUM_HEALTH, Behaviour.AggressiveMove);
            return u;
        }

        public static Unit CreateStrongMelee()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, SLOW_SPEED, STRONG_HEALTH, Behaviour.AggressiveMove);
            return u;
        }

        public static Unit CreateWeakRanged()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, FAST_SPEED, WEAK_HEALTH, Behaviour.BasicMove);
            return u;
        }

        public static Unit CreateMediumRanged()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, MEDIUM_SPEED, MEDIUM_HEALTH, Behaviour.EvasiveMove);
            return u;
        }

        public static Unit CreateStrongRanged()
        {
            Unit u = new Unit();
            u.LoadContent("testUnit", Vector2.Zero, MEDIUM_SPEED, STRONG_HEALTH, Behaviour.EvasiveMove);
            return u;
        }
    }
}