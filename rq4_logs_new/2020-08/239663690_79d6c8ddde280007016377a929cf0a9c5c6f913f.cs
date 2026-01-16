using Unity.Entities;

namespace Game {
    public class HighlightArchetype
    {
        public static EntityArchetype GetCommandCreate()
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            return entityManager.CreateArchetype(
                typeof(CommandCreateHighlight)
            );
        }

        public static EntityArchetype GetCommandRemove()
        {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            return entityManager.CreateArchetype(
                typeof(CommandRemoveHighlight)
            );
        }
    }
}