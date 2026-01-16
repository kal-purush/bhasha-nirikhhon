using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BossHealthBar : MonoBehaviour
{
    [SerializeField] private Slider slider;
    [SerializeField] private Transform boss;
    private int health;

    private void Start()
    {
        boss = GameObject.FindGameObjectWithTag("Boss").transform;
        health = boss.GetComponent<BossBehaviours>().life;
        SetMaxHealth(health);
    }

    void Update()
    {
        health = boss.GetComponent<BossBehaviours>().life;
        Debug.Log(health);
        SetHealth(health);
    }

    private void SetMaxHealth(int life)
    {
        slider.maxValue = life;
        slider.value = life;
    }
    private void SetHealth(int life)
    {
        slider.value = life;
    }
}