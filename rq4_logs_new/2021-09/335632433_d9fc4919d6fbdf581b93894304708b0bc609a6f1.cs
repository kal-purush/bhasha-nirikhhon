using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BossHPScript : MonoBehaviour
{
    public Slider bossHpSlieder;
    public int maxHp = 100,nowHp;
    void Start()
    {
        nowHp = maxHp;
        bossHpSlieder.maxValue = maxHp;
        bossHpSlieder.value = nowHp;
    }
    void OnTriggerEnter(Collider other)
    {
        if(other.gameObject.tag=="lastAttack")
        {
            Attack_1();
        }
    }
    public void Attack_1()
    {
        bossHpSlieder.value -= 10;
    }
}