using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ChefCount_M : MonoBehaviour
{
    private Chef_Y chef;
    private GameObject player;
    private Mission2_M m2m;
    private bool cock;

    void Start()
    {
        player = GameObject.Find("Player");
        m2m = player.GetComponent<Mission2_M>();
        chef = this.GetComponent<Chef_Y>();
    }

    void Update()
    {
        if (chef.HP <= 0 && !cock)
        {
            m2m.EnemyBreak();
            cock = true;
        }
    }
}