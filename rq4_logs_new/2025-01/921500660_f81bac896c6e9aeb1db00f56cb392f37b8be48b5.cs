using UnityEngine;

public class PlayerInput : MonoBehaviour
{
    [Header("Weapon Equip/Unequip")]
    public bool EKey = false;
    public float weaponEquipCoolTime = 0.5f;
    private float weaponEquipCurTime = 0.0f;

    private void Start()
    {
        weaponEquipCurTime = weaponEquipCoolTime;
    }

    private void Update()
    {
        InputE();
    }

    private void InputE()
    {
        if (weaponEquipCurTime < weaponEquipCoolTime)
        {
            weaponEquipCurTime += Time.deltaTime;
        }
        if (Input.GetKeyDown("e") && EKey == false && weaponEquipCurTime >= weaponEquipCoolTime)
        {
            EKey = true;
            weaponEquipCurTime = 0.0f;
        }
        else if (Input.GetKeyDown("e") && EKey == true && weaponEquipCurTime >= weaponEquipCoolTime)
        {
            EKey = false;
            weaponEquipCurTime = 0.0f;
        }
    }
}