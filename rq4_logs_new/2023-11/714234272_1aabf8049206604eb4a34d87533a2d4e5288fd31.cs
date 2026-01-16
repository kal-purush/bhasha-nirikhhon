using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class EnemyAttack : MonoBehaviour
{
    //[Range(1, 3)]
    //[SerializeField] int enemyType = 1;

    Rigidbody body;
    //Animator animator;
    GameObject player;

    void Awake()
    {
        body = GetComponent<Rigidbody>();
        //animator = GetComponent<Animator>();

        player = GameObject.FindWithTag("Player");
        
    }

    void ActivateHitbox()
    {
        this.gameObject.transform.GetChild(0).gameObject.SetActive(true);
    }

    void DeactivateHitbox()
    {
        this.gameObject.transform.GetChild(0).gameObject.SetActive(false);
    }

    void Whoop() //function to test animation event
    {
        Debug.Log("WHOOP");
    }
}