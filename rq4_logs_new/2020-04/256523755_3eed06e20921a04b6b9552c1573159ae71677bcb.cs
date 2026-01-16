using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class enemyBehavior : MonoBehaviour
{

    float atkDelay = 2f;
    bool isAttack = false;
    CapsuleCollider horn;

    void Start()
    {
        horn = GetComponentInChildren<CapsuleCollider>();
    }

    IEnumerator BugAttackDelay() //Testing attack function
    {
        yield return new WaitForSeconds(atkDelay);
        //BugAttack();
        horn.enabled = false;
        isAttack = false;
    }

    void BugAttack()
    {
        isAttack = true;
        horn.enabled = true;
        StartCoroutine(BugAttackDelay());


    }
    


    // Update is called once per frame
    void Update()
    {
        if (isAttack == false)
        {
            BugAttack();
        }
    }
}