/*
namespace Systems.Grid.GridGenerationGroup
{
    [UpdateAfter(typeof(GridExpansionPhaseSystem))]
    public class GridCollisionCheckPhaseSystem : GridGenerationSystemBase
    {
        protected override void OnCreate()
        {
            base.OnCreate();

            _systemTilesQuery.SetSharedComponentFilter(
                    new GridGenerationComponent(GridGenerationPhase.CollisionCheck)
                );
        }

        protected override void OnUpdate()
        {
            if (_systemTilesQuery.IsEmpty) return;

            #region ComputeMaxCounts

            var tileCount = _systemTilesQuery.CalculateEntityCount();

            #endregion
            #region InstantiateContainers

            // var tileComponentLookup = GetComponentDataFromEntity<TileComponent>(true);
            var tilesArray = _systemTilesQuery.ToEntityArray(Allocator.TempJob);

            #endregion
            #region CollisionCheck

            var processInitialTileLinksJob =
                new ProcessInitialTileLinksJob
                {
                    LinkingTilesArray = linkingTilesArray,
                    TileComponentLookup = tileComponentLookup,
                    CenterNodesMapWriter = centerNodesMap.AsParallelWriter()
                }.Schedule(centerNodesMaxCount, 1);
            Dependency = JobHandle.CombineDependencies(Dependency, processInitialTileLinksJob);

            #endregion
            #region CleanUp

            tilesArray.Dispose(Dependency);

            #endregion

            _ecbSystem.AddJobHandleForProducer(Dependency);
        }

        [BurstCompile]
        private struct DestroyTilesOnCollisionJob : IJobParallelFor
        {
            [ReadOnly]
            public NativeArray<Entity> TilesArray;
            [ReadOnly]
            public BufferFromEntity<StatefulTriggerEvent>
            [ReadOnly]
            public PhysicsWorld PhysicsWorld;

            [WriteOnly]
            public EntityCommandBuffer.ParallelWriter EcbWriter;

            public void Execute(int i)
            {
                var tile = TilesArray[i];

                PhysicsWorld.CollisionWorld.
            }
        }
    }
}*/
