using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class WeaponSystem : MonoBehaviour {
    List<GameObject> weaponsGOList;
    List<string> weaponNames;
    string previousWeapon, currentWeapon, nextWeapon;
    int weaponIndex;
    int weaponCount;
    PlayerShooting playerShootingScript;

    void Start()
    {
        playerShootingScript = GetComponent<PlayerShooting>();
        GameObject[] weaponsGO = GameObject.FindGameObjectsWithTag("Gun");
        weaponsGOList = new List<GameObject>();
        for (int i = 0; i < weaponsGO.Length; i++)
        {
            weaponsGO[i].SetActive(false);
            weaponsGOList.Add(weaponsGO[i]);
        }
        weaponIndex = 0;
        weaponNames = new List<string> { "MachineGun", "GravityGun", "ShotGun" };
        weaponCount = weaponNames.Count - 1;
        previousWeapon = weaponNames[weaponIndex];
        currentWeapon = weaponNames[++weaponIndex];
        nextWeapon = weaponNames[++weaponIndex];
        UpdateWeaponInHand();
    }

    void Update()
    {
        if (Input.GetAxis("Mouse ScrollWheel") > 0f || Input.GetKeyDown(KeyCode.Z))
        {
            previousWeapon = currentWeapon;
            currentWeapon = nextWeapon;
            if (weaponIndex < weaponCount)
            {
                nextWeapon = weaponNames[++weaponIndex];
            }
            else
            {
                weaponIndex = 0;
                nextWeapon = weaponNames[weaponIndex];
            }
            UpdateWeaponInHand();
        }
        else if (Input.GetAxis("Mouse ScrollWheel") < 0f || Input.GetKeyDown(KeyCode.X)) 
        {
            nextWeapon = currentWeapon;
            currentWeapon = previousWeapon;
            if (weaponIndex > 0)
            {
                previousWeapon = weaponNames[weaponIndex--];
            }
            else
            {
                weaponIndex = weaponCount;
                previousWeapon = weaponNames[weaponIndex];
            }
            UpdateWeaponInHand();
        }
    }
    void UpdateWeaponInHand()
    {
        for (int i = 0; i < weaponsGOList.Count; i++)
        {
            if (weaponsGOList[i].name == currentWeapon)
            {
                weaponsGOList[i].SetActive(true);
                playerShootingScript.anim = weaponsGOList[i].GetComponent<Animator>();
            }
            else if (weaponsGOList[i].name == nextWeapon || weaponsGOList[i].name == previousWeapon)
            {
                weaponsGOList[i].SetActive(false);
            }
        }
    }
}