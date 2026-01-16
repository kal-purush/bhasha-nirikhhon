using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class Inventory : MonoBehaviour {
    //CHA1 - Milda Petrikaitė IFF-6/5

    public GameObject[] inventory = new GameObject[10]; //inventory array to store items
    public Button[] InventoryButtons = new Button[10]; //array for display
    
    public void AddItem(GameObject item)
    {
        bool itemAdded = false; //to determine whether the item was added (for inv is full case)

        //find the first unoccupied slot in the inventory
        for(int i = 0; i < inventory.Length; i++)
        {
            if(inventory[i] == null)
            {
                //add item
                inventory[i] = item;
                //update UI
                InventoryButtons[i].GetComponentInChildren<Text>().text = "+"; //image.overrideSprite = item.GetComponent<SpriteRenderer>().sprite;
                Debug.Log(item.name + " was added");
                itemAdded = true;
                //do something with the object
                item.SendMessage("DoInteraction");
                break;
            }
        }

        //inventory was full
        if (!itemAdded)
        {
            Debug.Log("Inventory full");
        }
    }
}