using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Unity.Collections;
using Unity.Entities;
using Unity.Mathematics;
using Unity.Transforms;
using Hex.Coordinates;
using Game;

public class SettlerPanelButtons : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }

    public void CreateCity()
        {
            Debug.Log("Criação de cidade");
            EntityManager entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;

            Entity entity = SettlerEntity.GetSelected(entityManager);

            SettlerEntity.AddCommandCreateCity(entityManager, entity);

            // settlerPanelPrefab.SetActive(false);

            // if (panel != Entity.Null) {
            //     entityManager.DestroyEntity(panel);
            // }
        }

        public void BackButton()
        {
            Debug.Log("Destrua o painel");
            // settlerPanelPrefab.SetActive(false);
        }
}