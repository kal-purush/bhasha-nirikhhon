using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public enum BusinessFeatureTitle
{
    FeatureA,
    FeatureB,
    FeatureC,
    FeatureD
}

public class BusinessFeature : MonoBehaviour
{
    public BusinessFeatureTitle title;
    public string description;
    public List<StatChange> statChangesPerTurn;
    public List<StatPrerequisite> statPrerequisites;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    public bool CanBePurchased(PlayerStatIntDictionary playerStats)
    {
        //iterate over every stat prerequisite, check if it matches what is in input dict
        foreach (StatPrerequisite statPrerequisite in statPrerequisites)
        {
            if (statPrerequisite.statMustBeEqualOrGreater)
            {
                if (playerStats[statPrerequisite.stat] < statPrerequisite.targetValue) return false;
            }
            else
            {
                if (playerStats[statPrerequisite.stat] >= statPrerequisite.targetValue) return false;
            }
        }

        //no mismatches found, card's statprerequisites are met
        return true;
    }


}