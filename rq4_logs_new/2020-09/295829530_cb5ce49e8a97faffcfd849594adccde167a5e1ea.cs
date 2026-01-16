using System;
using System.Collections.Generic;

namespace Program
{
    // Clase que contendra todos los items equipados del personaje
    public class Inventory
    {
        List<Item> InventoryItems {get;set;}
        public Inventory(List<Item> inventory)
        {
            this.InventoryItems=inventory;
        }

        public void AddItem(Item item)
        {
            this.InventoryItems.Add(item);
        }
        public void RemoveItem(Item item)
        {
            this.InventoryItems.Remove(item);
        }
        public bool Contains(Item item)
        {
            bool res = false;
            foreach (Item searchedValue in this.InventoryItems)
            {
                res = (item==searchedValue) || res;
            }
            return res;
        }
    }
  
}