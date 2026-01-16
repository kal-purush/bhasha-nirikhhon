using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[CreateAssetMenu(fileName = "New Inventory", menuName = "InventorySystem/Inventory")]

public class InventoryObject : ScriptableObject
{
    public List<WeaponSlot> Container =  new List<WeaponSlot>();
    public void AddGear(Gear _gear, int _amount){
        bool has = false;

        for(int i = 0; i < Container.Count; i++){
            if(Container[i].weapon == _gear){
                has = true;
                Container[i].addAmount(_amount);
                break;
            }
        }

        if(has == false){
            Container.Add(new WeaponSlot(_gear, _amount));
        }

    }

    // Define the custom data type for equipment Weapon
    [System.Serializable] public class WeaponSlot
    {
        public Gear weapon;
        public int amount;
        public WeaponSlot(Gear _weapon, int _amount){
            weapon = _weapon;
            amount = _amount;
        }

        public void addAmount(int value){
            amount = amount + value;
        }

    }
}