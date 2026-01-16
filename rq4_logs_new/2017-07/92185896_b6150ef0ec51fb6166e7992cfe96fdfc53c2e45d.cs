using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class InventoryBox : MonoBehaviour
{

    public int boxNumber;

    public InventoryItem[] iItems;
    private static int currentItem;

    private static bool iBoxExists;






    // Use this for initialization
    void Start()
    {

        if (!iBoxExists)
        {
            iBoxExists = true;
        }
        else
        {
            if (iItems[currentItem] != null)
            {
                gameObject.SetActive(true);

                iItems[currentItem].gameObject.SetActive(true);
            }
        }

    }

    // Update is called once per frame
    void Update()
    {

    }

    public void ShowItem(string itemName)
    {
        for (int i = 0; i < iItems.Length; i++)
        {
            if (iItems[i].iItemName == itemName)
            {
                currentItem = i;
                gameObject.SetActive(true);
                iItems[i].gameObject.SetActive(true);
            }
        }
    }

    public void DesactiveBox()
    {
        iItems[currentItem] = null;
        gameObject.SetActive(false);
    }


}