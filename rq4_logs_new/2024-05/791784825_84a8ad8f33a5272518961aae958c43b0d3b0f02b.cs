using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerActions : MonoBehaviour
{

    public GameObject weaponMelee;
    public bool isAttacking;
    public int attackCombo;

    private void Awake()
    {
        EquipWeapon();
    }

    public void EquipWeapon()
    {
        foreach (Transform child in gameObject.transform)
        {
            if (child.CompareTag("WeaponMelee"))
            {
                weaponMelee = child.gameObject;
            }
        }
    }

    public void Attack()
    {
        if (weaponMelee !=null)
        {
            StartCoroutine(AttempToAttack());
        }

    }

    private IEnumerator AttempToAttack()
    {
        weaponMelee.SetActive(true);
        yield return new WaitForSeconds(0.1f);
    }

}