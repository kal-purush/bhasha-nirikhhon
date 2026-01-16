using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AttackRestrictions_R : MonoBehaviour
{
    // 攻撃の制限
    private static AttackRestrictions_R instance = new AttackRestrictions_R();
    private float timer;

    public static AttackRestrictions_R GetInstance()
    {
        return instance;
    }

    public void Update()
    {
        if (timer > 0f)   
            timer -= Time.deltaTime;
    }

    public bool CanAttack()
    {
        Debug.Log(timer <= 0.0f);
        return timer <= 0.0f;
    }

    public void SetTimer(float time)
    {
        Debug.Log(time);
        timer = time;
    }
}