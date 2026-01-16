using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;

public class ChestManager : MonoBehaviour
{
    // public List<ItemInventory> items = new List<ItemInventory>();
    // // public GameObject inventoryBackground;
    // // public GameObject chestBackground;
    // public InventoryManager inventoryManager;

    // void Start()
    // {
    //     inventoryManager.AddGraphics(this);
    // }

    // void OnTriggerStay2D(Collider2D other)
    // {
    //     if (other.gameObject.name == "Player")
    //     {
    //         if (Input.GetKey(KeyCode.E) && !UIEventHandler.gameIsPaused)
    //         {
    //             // inventoryBackground.SetActive(!inventoryBackground.activeSelf);
    //             // chestBackground.SetActive(!chestBackground.activeSelf);

    //             // if (inventoryBackground.activeSelf)
    //             // {
    //             //     UpdateInventory();
    //             // }

    //             // Time.timeScale = inventoryBackground.activeSelf ? 0f : 1f;

    //             inventoryManager.OpenChest();
    //             UpdateChest();
    //         }
    //     }
    // }

    // public void UpdateChest()
    // {
    //     for (int i = 0; i < inventoryManager.maxCount; i++)
    //     {
    //         if (items[i].id != 0 && items[i].count > 1)
    //         {
    //             items[i].itemGameObj.GetComponentInChildren<Text>().text = items[i].count.ToString();
    //         }
    //         else
    //         {
    //             items[i].itemGameObj.GetComponentInChildren<Text>().text = "";
    //         }

    //         items[i].itemGameObj.GetComponent<Image>().sprite = inventoryManager.data.items[items[i].id].img;
    //     }
    // }

    // public void AddItem(int id, Item item, int count)
    // {
    //     items[id].id = item.id;
    //     items[id].count = count;
    //     items[id].itemGameObj.GetComponent<Image>().sprite = item.img;

    //     if (count > 1 && item.id != 0)
    //     {
    //         items[id].itemGameObj.GetComponentInChildren<Text>().text = count.ToString();
    //     }
    //     else
    //     {
    //         items[id].itemGameObj.GetComponentInChildren<Text>().text = "";
    //     }
    // }

    // public void AddChestItem(int id, ItemInventory invItem)
    // {
    //     items[id].id = invItem.id;
    //     items[id].count = invItem.count;
    //     items[id].itemGameObj.GetComponent<Image>().sprite = inventoryManager.data.items[invItem.id].img;

    //     if (invItem.count > 1 && invItem.id != 0)
    //     {
    //         items[id].itemGameObj.GetComponentInChildren<Text>().text = invItem.count.ToString();
    //     }
    //     else
    //     {
    //         items[id].itemGameObj.GetComponentInChildren<Text>().text = "";
    //     }
    // }
}