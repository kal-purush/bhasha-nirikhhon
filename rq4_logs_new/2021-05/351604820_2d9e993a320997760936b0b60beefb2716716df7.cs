using Components.Movement;
using Components.Tags.Selection;
using Unity.Entities;
using UnityEngine;

namespace Entities
{
    public class Unit : MonoBehaviour, IConvertGameObjectToEntity
    {
        public void Convert(
            Entity entity,
            EntityManager entityManager,
            GameObjectConversionSystem conversionSystem
        ) {
            var pos = transform.position;

            entityManager.AddComponentData(
                entity,
                new UnitComponent{
                    Position = pos,
                    Destination = pos
                }
            );
            entityManager.AddComponentData(
                entity,
                new SelectableTag()
            );
        }
    }
}