using UnityEngine;
using System.Collections.Generic;
using System.IO;
using LitJson;

public class ItemDatabase : MonoBehaviour
{
    //private List<Collectables> collectable = new List<Collectables>();
    private List<Weapon> weaponDatabase = new List<Weapon>();
    private string jsonString;
    private JsonData itemData;

    // Use this for initialization
    void Start()
    {
        jsonString = File.ReadAllText(Application.dataPath + "/Resources/Items.json");
        itemData = JsonMapper.ToObject(jsonString);
        loadWeapons();
        Debug.Log(searchWeapons(0).itemName);     
    }

    private void loadWeapons()
    {
        for (int i = 0; i < itemData["Weapons"].Count; i++)
        {
            weaponDatabase.Add(new Weapon((int)itemData["Weapons"][i]["id"], itemData["Weapons"][i]["title"].ToString(),
                itemData["Weapons"][i]["description"].ToString(), (int)itemData["Weapons"][i]["power"],
                itemData["Weapons"][i]["type"].ToString(), itemData["Weapons"][i]["slug"].ToString()));
        }        
    }

    public Weapon searchWeapons(int id)
    {
        foreach(var item in weaponDatabase)
        {
            if (item.id == id)
            {
                return item;
            }
        }

        return null;
    }
}