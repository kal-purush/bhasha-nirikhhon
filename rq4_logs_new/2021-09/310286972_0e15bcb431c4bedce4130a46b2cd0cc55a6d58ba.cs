using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SmallHit_M : MonoBehaviour
{
    public GameObject player;
    public Mission1_M m1m;
    public bool hit;

    void Start()
    {
        player = GameObject.Find("Player");
        m1m = player.GetComponent<Mission1_M>();
    }

    void OnCollisionEnter(Collision collision)//tag big
    {
        if (collision.gameObject.tag == "Big")
        { 
            if (!hit)
            {
                Debug.Log("成功");
                m1m.ShardAttack();
                hit = true;
            }
        }
    }
}