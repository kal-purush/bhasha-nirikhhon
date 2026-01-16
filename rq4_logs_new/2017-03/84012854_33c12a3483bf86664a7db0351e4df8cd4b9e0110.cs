using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HealthPotion : UseItem {


    protected override void OnUse()
    {
        
        if(owner != null)
        {
            owner.mystats.Heal(1);
            owner.myUi.UpdateCurrentHealth();
            uses--;
        }
        if(uses <= 0)
        {
            owner.DestroyUseItem(this);  
        }
    }



    // Use this for initialization
    void Start () {
        InitStats();
		
	}
	

    void InitStats()
    {
        ItemName = itemNamex;
        base.Init();
        img = vImage;
        Desc = description;
        Id = idx;
        uses = 1;
        Type = Itemtype.use;
    }
	// Update is called once per frame
	void Update () {
		
	}
}