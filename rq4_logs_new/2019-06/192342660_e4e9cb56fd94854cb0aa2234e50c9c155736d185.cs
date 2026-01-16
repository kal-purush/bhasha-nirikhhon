using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Inventory : MonoBehaviour
{
    public Hashtable items = new Hashtable();
    string itemName = "";


    public void OnTriggerEnter2D(Collider2D collision)
    {
        itemName = collision.name;

        if (collision.tag == "item")
        {
            items.Add(itemName, "1");
            Debug.Log(collision.name);
            Destroy(GameObject.Find(itemName));
        }
        else
        {
            Debug.Log(collision.name);
        }
    }

    public void showInventory()
    {
        foreach (string item in items.Keys)
        {
            Debug.Log(item);
        }

    }
}