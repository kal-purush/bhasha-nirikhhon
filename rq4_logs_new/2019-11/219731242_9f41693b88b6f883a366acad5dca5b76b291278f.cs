using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SkoleProjekt_GodtNavn
{
    public class Equipment
    {
        public Item helmet;
        public Item chest;
        public Item leg;
        public Item gloves;
        public Item boots;
        public Item weapon;
        private Player player;


        public void EquipItem(Item item)
        {
            switch (item.ItemType)
            {
                case ItemType.Helmet:
                    SwitchItem(item, ItemType.Helmet);
                    break;
                case ItemType.Chest:
                    SwitchItem(item, ItemType.Chest);
                    break;
                case ItemType.Leg:
                    SwitchItem(item, ItemType.Leg);
                    break;
                case ItemType.Gloves:
                    SwitchItem(item, ItemType.Gloves);
                    break;
                case ItemType.Boots:
                    SwitchItem(item, ItemType.Boots);
                    break;
                case ItemType.WeaponRange:
                    SwitchItem(item, ItemType.WeaponRange);
                    break;
                case ItemType.WeaponMelee:
                    SwitchItem(item, ItemType.WeaponMelee);
                    break;
                default:
                    Console.WriteLine("Equipment Error");
                    break;
            }
        }

        public void SwitchItem(Item item, ItemType itemType)
        {
            #region Helment
            if(ItemType.Helmet == itemType)
            if (helmet != null)
            {
                helmet = item;
            }
            else
            {
                //Gameworld.player.inventory.AddItem(helmet);
                helmet = item;
            }
            #endregion
            #region Chest
            if (ItemType.Chest == itemType)
                if (chest != null)
                {
                    chest = item;
                }
                else
                {
                    //Gameworld.player.inventory.AddItem(chest);
                    chest = item;
                }
            #endregion
            #region Leg
            if (ItemType.Leg == itemType)
                if (leg != null)
                {
                    leg = item;
                }
                else
                {
                    //Gameworld.player.inventory.AddItem(leg);
                    leg = item;
                }
            #endregion
            #region Gloves
            if (ItemType.Gloves == itemType)
                if (gloves != null)
                {
                    gloves = item;
                }
                else
                {
                    //Gameworld.player.inventory.AddItem(gloves);
                    gloves = item;
                }
            #endregion
            #region Boots
            if (ItemType.Boots == itemType)
                if (boots != null)
                {
                    boots = item;
                }
                else
                {
                    //Gameworld.player.inventory.AddItem(boots);
                    boots = item;
                }
            #endregion
            #region Weapon
            if (ItemType.WeaponMelee == itemType || ItemType.WeaponMelee == itemType)
                if (weapon != null)
                {
                    weapon = item;
                }
                else
                {
                    //Gameworld.player.inventory.AddItem(weapon);
                    weapon = item;
                }
            #endregion
        }

        public void UnEquipItem(Item item)
        {
            switch (item.ItemType)
            {
                case ItemType.Helmet:
                    //Gameworld.player.inventory.AddItem(helmet);
                    helmet = null;
                    break;
                case ItemType.Chest:
                    //Gameworld.player.inventory.AddItem(chest);
                    chest = null;
                    break;
                case ItemType.Leg:
                    //Gameworld.player.inventory.AddItem(leg);
                    leg = null;
                    break;
                case ItemType.Gloves:
                    //Gameworld.player.inventory.AddItem(gloves);
                    gloves = null;
                    break;
                case ItemType.Boots:
                    //Gameworld.player.inventory.AddItem(boots);
                    boots = null;
                    break;
                case ItemType.WeaponRange:
                    //Gameworld.player.inventory.AddItem(weapon);
                    weapon = null;
                    break;
                case ItemType.WeaponMelee:
                    //Gameworld.player.inventory.AddItem(weapon);
                    weapon = null;
                    break;
                default:
                    Console.WriteLine("Equipment Error");
                    break;
            }
        }
    }
}