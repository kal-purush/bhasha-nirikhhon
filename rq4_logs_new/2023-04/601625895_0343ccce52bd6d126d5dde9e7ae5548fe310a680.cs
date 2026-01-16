using UnityEngine;

public class InventorySlot : MonoBehaviour
{
    private string itemName;
    private WeaponData data;

    public InventorySlot(string itemName, WeaponData data)
    {
        this.itemName = itemName;
        this.data = data;
    }

    public string GetName(){
        return this.itemName;
    }

    public WeaponData GetData(){
        return this.data;
    }

    public void SetName(string itemName){
        this.itemName = itemName;
    }

    public void SetData(WeaponData data){
        this.data = data;
    }
}