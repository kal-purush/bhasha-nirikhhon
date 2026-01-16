using Systems.Controls;
using Components.HexGrid;
using Components.Tags.Selection;
using Unity.Collections;
using Unity.Entities;
using Unity.Rendering;
using UnityEngine;

namespace Systems.HexGrid
{
    [UpdateInGroup(typeof(FixedStepSimulationSystemGroup))]
    [UpdateAfter(typeof(ControlSystemGroup))]
    [UpdateBefore(typeof(EndFixedStepSimulationEntityCommandBufferSystem))]
    public class TileSelectionStateSystem : SystemBase
    {
        private RenderMesh _defaultRenderMesh;
        private EndFixedStepSimulationEntityCommandBufferSystem _ecbSystem;
        private RenderMesh _selectedRenderMesh;

        protected override void OnStartRunning()
        {
            _ecbSystem =
                World.GetExistingSystem<EndFixedStepSimulationEntityCommandBufferSystem>();

            var path = "Prefabs/Utils/Materials/";
            var defaultTileMat = Resources.Load<Material>(path + "DefaultTileMat");
            var selectedTileMat = Resources.Load<Material>(path + "SelectedTileMat");

            var renderMeshEntityQuery = GetEntityQuery(
                ComponentType.ReadOnly<HexTileComponent>(),
                ComponentType.ReadOnly<RenderMesh>()
            );
            var entityArray = renderMeshEntityQuery.ToEntityArray(Allocator.Temp);
            var tmp = entityArray[0];

            _defaultRenderMesh = EntityManager.GetSharedComponentData<RenderMesh>(tmp);
            _defaultRenderMesh.material = defaultTileMat;
            _selectedRenderMesh = EntityManager.GetSharedComponentData<RenderMesh>(tmp);
            _selectedRenderMesh.material = selectedTileMat;

            entityArray.Dispose();
        }

        protected override void OnUpdate()
        {
            UpdateDefaultToSelectedRenderMesh(_defaultRenderMesh, _selectedRenderMesh);
            UpdateSelectedToDefaultRenderMesh(_defaultRenderMesh, _selectedRenderMesh);

            _ecbSystem.AddJobHandleForProducer(Dependency);
        }

        private void UpdateDefaultToSelectedRenderMesh(
            RenderMesh defaultRenderMesh,
            RenderMesh selectedRenderMesh
        )
        {
            var parallelWriter = _ecbSystem.CreateCommandBuffer();

            Entities.WithAll<HexTileComponent, SelectedTag>()
                .ForEach(
                    (
                        Entity entity
                    ) =>
                    {
                        parallelWriter.SetSharedComponent(entity, selectedRenderMesh);
                    }
                )
                .WithoutBurst()
                .Run();
        }

        private void UpdateSelectedToDefaultRenderMesh(
            RenderMesh defaultRenderMesh,
            RenderMesh selectedRenderMesh
        )
        {
            var parallelWriter = _ecbSystem.CreateCommandBuffer();

            Entities.WithAll<HexTileComponent>()
                .WithNone<SelectedTag>()
                .ForEach(
                    (
                        Entity entity,
                        int entityInQueryIndex
                    ) =>
                    {
                        parallelWriter.SetSharedComponent(entity, defaultRenderMesh);
                    }
                )
                .WithoutBurst()
                .Run();
        }
    }
}