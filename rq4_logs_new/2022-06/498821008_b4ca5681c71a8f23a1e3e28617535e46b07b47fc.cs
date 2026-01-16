using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Weapon : MonoBehaviour
{
    // Start is called before the first frame update
    public int WeaponSwithc = 0;
    void Start()
    {
        SelectWeapon();
    }

    // Update is called once per frame
    void Update()
    {
        int cuuurentWeapon = WeaponSwithc;
       if(Input.GetAxis("Mouse ScrollWheel") > 0f)
        {
            if(WeaponSwithc >= transform.childCount - 1)
            {
                WeaponSwithc = 0;
            }
            else
            {
                WeaponSwithc++;
            }
        }
        if (Input.GetAxis("Mouse ScrollWheel") < 0f)
        {
            if (WeaponSwithc <= 0)
            {
                WeaponSwithc =transform.childCount-1;
            }
            else
            {
                WeaponSwithc--;
            }
        }
        if (Input.GetKeyDown(KeyCode.Alpha1))
        {
            WeaponSwithc = 0;
        }
        if (Input.GetKeyDown(KeyCode.Alpha2) && transform.childCount >=2)
        {
            WeaponSwithc = 1;
        }
        if (Input.GetKeyDown(KeyCode.Alpha3) && transform.childCount >= 3)
        {
            WeaponSwithc = 2;
        }
        if (cuuurentWeapon!=WeaponSwithc)
        {
            SelectWeapon();
        }
    }

    void SelectWeapon()
    {
        int i = 0;
        foreach (Transform weapon in transform)
        {
            if (i == WeaponSwithc)
                weapon.gameObject.SetActive(true);
            else
                weapon.gameObject.SetActive(false);
            i++;
        }
    }
}