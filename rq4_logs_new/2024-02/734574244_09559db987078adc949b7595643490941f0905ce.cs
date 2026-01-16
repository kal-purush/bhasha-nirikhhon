using Suhdo.CharacterCore;
using UnityEngine;

namespace Suhdo
{
    public abstract class BaseCollisionSenses : CoreComponent
    {
        public abstract LayerMask WhatIsGround {get;}
        public abstract float WallFrontCheckDistance { get;}
        public abstract float LedgeCheckDistance { get; }
        
        public abstract Transform GroundCheck { get; }
        public abstract Transform WallCheck { get; }
        public abstract Transform CeilingCheck { get; }
        public abstract Transform LedgeCheck { get; }
        
        public abstract bool Ground { get; }
        public abstract bool Ceiling { get; }
        public abstract bool WallFront { get; }
        public abstract bool WallBack { get; }
        public abstract bool Ledge { get; }
    }
}