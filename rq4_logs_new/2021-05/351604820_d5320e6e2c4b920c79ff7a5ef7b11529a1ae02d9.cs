using System;
using Components.Movement;
using Unity.Entities;
using Unity.Mathematics;
using UnityEngine;

namespace Entities
{
    public class Actor : MonoBehaviour, IConvertGameObjectToEntity
    {
        public void Convert(
            Entity entity,
            EntityManager entityManager,
            GameObjectConversionSystem conversionSystem
        ) {
            var pos = transform.position;
            var gridPos = new float3(
                math.floor(pos.x),
                0,
                math.floor(pos.z)
            );
            
            entityManager.AddComponentData(
                entity,
                new ActorComponent{
                    Position = gridPos,
                    Destination = gridPos
                }
            );
        }
    }
}