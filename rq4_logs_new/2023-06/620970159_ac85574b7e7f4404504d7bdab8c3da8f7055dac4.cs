// script to manage upgrading weapons
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

public class UpgradeWeapon : MonoBehaviour
{
    [SerializeField] Weapon weapon;
    [SerializeField] WeaponInventory weaponInventory;
    [SerializeField] ResourceInventory resourceInventory;
    [SerializeField] Message message;

    [SerializeField] GameObject[] upgradeSlots;
    private int currentSlot = 0;

    [SerializeField] Resource wood;
    [SerializeField] Resource rock;

    [SerializeField] TMP_Text woodReqText;
    [SerializeField] TMP_Text rockReqText;

    private int woodReq = 10;
    private int rockReq = 5;

    // Start is called before the first frame update
    void Start()
    {
        woodReqText.text = $"{woodReq}";
        rockReqText.text = $"{rockReq}";
      
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void CheckIfCanUpgrade()
    {
        // check if weapon is not already built
        if(CheckIfWeaponIsBuilt() == false)
        {
            message.Send($"You do not have a {weapon.name} in your inventory");
        } else if(upgradeSlots.Length - 1 == currentSlot) // check if player has completely upgraded the weapon
        {
            message.Send($"The {weapon.name} is fully upgraded!");
        } else
        {
            // current player resources
            int currentWood = resourceInventory.GetResourceCount(wood);
            int currentRock = resourceInventory.GetResourceCount(wood);

            // check if player has enough resources
            if (currentWood >= woodReq && currentRock >= rockReq)
            {
                // add 10% more damage to the player's weapon
                weapon.damage += 10;

                // add upgraded slot visibility
                upgradeSlots[currentSlot].gameObject.SetActive(true);
                currentSlot++;

                // update wood and rock requirements for next upgrade
                woodReqText.text = $"{woodReq *= 3}";
                rockReqText.text = $"{rockReq *= 3}";

            }
            else
            {
                // send error message
                message.Send($"You do not have enough resources to upgrade the {weapon.name}!");
            }
        }
    }

    // function to check if the player has built the referenced weapon
    private bool CheckIfWeaponIsBuilt() 
    {
        return weaponInventory.weapons.TryGetValue(weapon, out bool value);
    }
}