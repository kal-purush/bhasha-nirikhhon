using System;
using Unity.Entities;
using Unity.Mathematics;

namespace Components.Events.Physics
{
    [Serializable]
    public struct StatefulCollisionEvent : IBufferElementData
    {
        public float3 Normal;
        public bool HasCollisionDetails;
        public float3 AverageContactPointPosition;
        public float EstimatedImpulse;
        public PhysicsEventState State;

        // ReSharper disable once InconsistentNaming
        public bool _isStale;
        public Entity Entity;
    }
}