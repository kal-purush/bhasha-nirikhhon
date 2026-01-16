using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AbilityBarInitializer : MonoBehaviour
{

    public AbilityUIComponent abilityUIComponentPrefab;

    public void init()
    {
        var player = FindObjectOfType<Playercontroller>();

        foreach (var ability in player.abilitys)
        {
            var newprefab = Instantiate(abilityUIComponentPrefab);
            newprefab.ability = ability;
            newprefab.transform.parent = gameObject.transform;

        }
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}