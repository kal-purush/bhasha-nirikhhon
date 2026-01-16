using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_Craft001_OreExtractor : Scr_CraftingResultBase
{

    [SerializeField] private Scr_ReferenceManager referenceManager;
    [SerializeField] private GameObject playerShip;

    public override void ObjectGeneration()
    {
        for (int i = 0; i < playerShip.GetComponent<Scr_PlayerShipStats>().toolWarehouse.Length; i++)
        {
            if (playerShip.GetComponent<Scr_PlayerShipStats>().toolWarehouse[i] == null)
            {
                playerShip.GetComponent<Scr_PlayerShipStats>().toolWarehouse[i] = referenceManager.OreExtractor;
                break;
            }
        }
    }

    public override void MakingImprovement()
    {

    }
}