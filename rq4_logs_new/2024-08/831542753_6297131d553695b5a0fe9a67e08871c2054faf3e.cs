using System.Collections.Generic;
using UnityEngine;

public class Inventory : MonoBehaviour
{
    private static Inventory instance;
    private Dictionary<Item.ItemType, int> inventoryItems = new Dictionary<Item.ItemType, int>();

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }
    }

    public static void AddItem(Item.ItemType itemType)
    {
        if (instance.inventoryItems.ContainsKey(itemType))
        {
            instance.inventoryItems[itemType]++;
        }
        else
        {
            instance.inventoryItems[itemType] = 1;
        }
    }

    public static bool HasItem(Item.ItemType itemType)
    {
        return instance.inventoryItems.ContainsKey(itemType) && instance.inventoryItems[itemType] > 0;
    }

    public static int GetItemCount(Item.ItemType itemType)
    {
        return instance.inventoryItems.ContainsKey(itemType) ? instance.inventoryItems[itemType] : 0;
    }

    public static Dictionary<Item.ItemType, int> GetAllItems()
    {
        return new Dictionary<Item.ItemType, int>(instance.inventoryItems);
    }
}