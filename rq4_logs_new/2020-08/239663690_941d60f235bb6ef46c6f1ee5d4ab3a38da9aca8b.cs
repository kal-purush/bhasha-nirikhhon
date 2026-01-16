using System;
using UnityEngine;
using Unity.Entities;
using Unity.Mathematics;
using Unity.Rendering;
using Unity.Transforms;
using Unity.Collections;

namespace GameUI.Events
{
    public class CreateCityEvent : MonoBehaviour {
        // SettlerPanelManager settlerPanelManager;
        private void Start() {
            // settlerPanelManager = GetComponent<SettlerPanelManager>();
            // settlerPanelManager.OnCreateCity += OnCreateCity;
        }

        protected virtual void OnCreateCity(object sender, CreateCityEventArgs createCityArgs) {
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            // Entity entity = SettlerEntity.GetSelected(entityManager);

            // SettlerEntity.AddCommandCreateCity(entityManager, entity, createCityArgs);
        }
    }


}