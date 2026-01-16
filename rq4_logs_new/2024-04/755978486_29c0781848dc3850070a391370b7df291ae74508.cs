using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Assets.Scripts.Combat;
using Assets.Scripts.Capabilities;

public class EnemySkillBar : MonoBehaviour
{
    [SerializeField] private GameObject _robot;

    [SerializeField] private EquipmentSlot _swordSlot;
    [SerializeField] private EquipmentSlot _gunSlot;

    [SerializeField] private EquipmentSlot _doubleJumpSlot;
    [SerializeField] private EquipmentSlot _dashSlot;

    private void Awake()
    {
        _swordSlot.gameObject.SetActive(_robot.GetComponent<Sword>()!=null);
        _gunSlot.gameObject.SetActive(_robot.GetComponent<Gun>() != null);

        _doubleJumpSlot.gameObject.SetActive(_robot.TryGetComponent(out Jump jump) && jump.GetMaxAirJump() == 1);
        _dashSlot.gameObject.SetActive(_robot.TryGetComponent(out Move move) && move._hasDash);
    }

    void Update()
    {
        gameObject.SetActive(_robot != PlayerManager.Instance.Player);
    }
}