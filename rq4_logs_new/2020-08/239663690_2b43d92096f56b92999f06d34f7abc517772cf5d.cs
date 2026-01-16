using Unity.Entities;
using Hex.Coordinates;

namespace Game
{
    public class SelectUnit
    {
        public static void Create(Entity entity)
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            EntityArchetype archetype = entityManager.CreateArchetype(
                typeof(CommandSelectUnit),
                typeof(HexCoordinates)
            );

            Entity selectEntity = entityManager.CreateEntity(archetype);

            HexCoordinates coordinates = entityManager.GetComponentData<HexCoordinates>(entity);

            entityManager.AddComponentData<CommandSelectUnit>(selectEntity, new CommandSelectUnit {});
            entityManager.AddComponentData<HexCoordinates>(selectEntity, coordinates);
        }
    }
}