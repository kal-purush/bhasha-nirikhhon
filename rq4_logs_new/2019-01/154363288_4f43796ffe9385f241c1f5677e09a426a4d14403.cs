using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_PlayerShipLaboratory : MonoBehaviour
{ 
    [Header("References")]
    [SerializeField] private Scr_UpgradeList upgradeData;

    private Scr_PlayerShipCraft playerShipCraft;

    private void Start()
    {
        playerShipCraft = GetComponent<Scr_PlayerShipCraft>();
    }

    public void TechnologyClic(int index)
    {
        if (!upgradeData.UpgradeList[index].activated)
        {
            playerShipCraft.InventoryInfo();

            List<string> keyr = new List<string>(playerShipCraft.Resources.Keys);

            foreach (string k in keyr)
            {
                if (upgradeData.UpgradeList[index].Resources[k] > playerShipCraft.Resources[k])
                {
                    //Codigo desactivando el boton
                    break;
                }
            }
        }

        else
            Debug.Log(0); //Codigo desactivando el boton
    }
}