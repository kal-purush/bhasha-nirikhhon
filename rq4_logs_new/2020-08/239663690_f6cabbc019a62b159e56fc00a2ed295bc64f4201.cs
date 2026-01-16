using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Unity.Collections;
using Unity.Entities;
using Unity.Mathematics;
using Unity.Transforms;
using Hex.Coordinates;
using Game;
using GameUI.Entities;

namespace GameUI.View
{
    public class SettlerPanelManager : MonoBehaviour
    {
        public GameObject settlerPanelPrefab;
        EntityManager entityManager;
        EntityQuery query;
        Entity panel;

        void Awake()
        {
            entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;
        }

        void Start()
        {
            settlerPanelPrefab.SetActive(false);
        }

        void Update()
        {

        }

        void LateUpdate()
        {
            query = entityManager.CreateEntityQuery(
                ComponentType.ReadOnly<SettlerPanelTag>(),
                ComponentType.ReadOnly<HexCoordinates>()
            );

            panel = Entity.Null;

            if (query.CalculateEntityCount() > 0) {
                // Show panel
                NativeArray<Entity> panels = query.ToEntityArray(Allocator.TempJob);
                panel = panels[0];

                settlerPanelPrefab.SetActive(true);
                // Set Panel attributes in Value Object

                panels.Dispose();
                return;
            }

            settlerPanelPrefab.SetActive(false);
        }

        // public void CreateCity()
        // {
        //     Debug.Log("Criação de cidade");
        //     EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

        //     Entity entity = SettlerEntity.GetSelected(entityManager);

        //     SettlerEntity.AddCommandCreateCity(entityManager, entity);

        //     settlerPanelPrefab.SetActive(false);

        //     if (panel != Entity.Null) {
        //         entityManager.DestroyEntity(panel);
        //     }
        // }

        // public void BackButton()
        // {
        //     Debug.Log("Destrua o painel");
        //     settlerPanelPrefab.SetActive(false);
        // }
    }
}