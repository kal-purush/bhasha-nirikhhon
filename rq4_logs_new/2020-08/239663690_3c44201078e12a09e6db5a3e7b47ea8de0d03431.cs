using Unity.Collections;
using Unity.Entities;
using Hex.Coordinates;

namespace GameUI.Entities
{
    public class UISettlerPanel
    {
        public static void Show(EntityCommandBuffer ecb, EntityArchetype archetype, HexCoordinates coordinates)
        {
            Entity panelEntity = ecb.CreateEntity(archetype);

            ecb.AddComponent<SettlerPanelTag>(panelEntity, new SettlerPanelTag {});
            ecb.AddComponent<HexCoordinates>(panelEntity, coordinates);
        }

        public static void Hide()
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;
            EntityQuery query = UISettlerPanel.GetQuery();

            if (query.CalculateEntityCount() > 0) {
                NativeArray<Entity> panels = query.ToEntityArray(Allocator.TempJob);
                Entity panel = panels[0];

                entityManager.DestroyEntity(panel);
                panels.Dispose();
            }
        }

        public static EntityArchetype GetArchetype()
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            return entityManager.CreateArchetype(
                typeof(SettlerPanelTag),
                typeof(HexCoordinates)
            );
        }

        public static EntityQuery GetQuery()
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            return entityManager.CreateEntityQuery(
                ComponentType.ReadOnly<SettlerPanelTag>(),
                ComponentType.ReadOnly<HexCoordinates>()
            );
        }
    }
}