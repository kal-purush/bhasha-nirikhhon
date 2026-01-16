using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class EquipItem : BaseItem
{
    public enum itemPlace
    {
        head,
        torso,
        lefth,
        righth,
        legs
    }
    [SerializeField]
    int id;

    [SerializeField]
    string itemName;

    [SerializeField]
    int str;

    [SerializeField]
    int def;

    [SerializeField]
    int hp;

    [SerializeField]

    public itemPlace equipPlace;



    StatsClass itemStats;

    public StatsClass ItemStats
    {
        get
        {
            return itemStats;
        }

        set
        {
            itemStats = value;
        }
    }



    void InitStats()
    {
        ItemName = itemName;
        Id = id;
        itemStats = new StatsClass();
        itemStats.Strength = str;
        itemStats.Defense = def;
        itemStats.Health = hp;

    }
}