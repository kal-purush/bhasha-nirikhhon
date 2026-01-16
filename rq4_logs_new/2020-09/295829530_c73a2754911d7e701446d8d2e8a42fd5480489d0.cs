using System;
using System.Collections.Generic;

namespace Program
{
    // Clase que contendra todos los items equipados del personaje
    public class Body
    {
        public Item Head { get; set; }
        public Item Chest { get; set; }
        public Item MainWeapon { get; set; }
        public Item SecondaryWeapon { get; set; }
        public Item Legs { get; set; }
        public Item Feet { get; set; }

        public Body(List<Item> items)
        {
            foreach (Item item in items)
            {
                switch (item.Type.ToLower())
                {
                    case ("head"):
                        this.Head = item;
                        break;
                    case ("chest"):
                        this.Chest = item;
                        break;
                    case ("mainweapon"):
                        this.MainWeapon = item;
                        break;
                    case ("secondaryweapon"):
                        this.SecondaryWeapon = item;
                        break;
                    case ("legs"):
                        this.Legs = item;
                        break;
                    case ("feet"):
                        this.Feet = item;
                        break;
                }
            }
        }

        public static List<Item> EquippedItems(Body body)
        // Metodo que busca que items tiene equipado el personaje y los agrega a una lista
        {
            List<Item> equippedItems = new List<Item> { };
            if (body.Head != null)
            {
                equippedItems.Add(body.Head);
            }
            if (body.Chest != null)
            {
                equippedItems.Add(body.Chest);
            }
            if (body.MainWeapon != null)
            {
                equippedItems.Add(body.MainWeapon);
            }
            if (body.SecondaryWeapon != null)
            {
                equippedItems.Add(body.SecondaryWeapon);
            }
            if (body.Legs != null)
            {
                equippedItems.Add(body.Legs);
            }
            if (body.Feet != null)
            {
                equippedItems.Add(body.Feet);
            }
            return equippedItems;
        }
    }
}