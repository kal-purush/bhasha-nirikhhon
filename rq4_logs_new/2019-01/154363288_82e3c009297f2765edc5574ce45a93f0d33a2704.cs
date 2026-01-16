using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Scr_WorkshopFilters : MonoBehaviour
{

    [SerializeField] private Scr_PlayerShipLaboratory playerShipLaboratory;
    [SerializeField] private Scr_CraftData craftData;

    private void Start()
    {
        FilterTools();
    }

    public void FilterTools()
    {
        for(int i = 0; i < playerShipLaboratory.buttonArray.Length; i++)
        {
            if(craftData.CraftList[i].craftType == CraftType.tools)
                playerShipLaboratory.buttonArray[i].SetActive(true);

            else
                playerShipLaboratory.buttonArray[i].SetActive(false);
        }
    }

    public void FilterStory()
    {
        for (int i = 0; i < playerShipLaboratory.buttonArray.Length; i++)
        {
            if (craftData.CraftList[i].craftType == CraftType.story)
                playerShipLaboratory.buttonArray[i].SetActive(true);

            else
                playerShipLaboratory.buttonArray[i].SetActive(false);
        }
    }

    public void FilterSpaceSuit()
    {
        for (int i = 0; i < playerShipLaboratory.buttonArray.Length; i++)
        {
            if (craftData.CraftList[i].craftType == CraftType.spaceSuit)
                playerShipLaboratory.buttonArray[i].SetActive(true);

            else
                playerShipLaboratory.buttonArray[i].SetActive(false);
        }
    }
}