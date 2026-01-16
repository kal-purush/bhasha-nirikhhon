using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HeartUI : MonoBehaviour
{
    //hearts, from left to right
    public List<GameObject> hearts;

    // Update is called once per frame
    void Update()
    {
        int health = Player.instance.health;
        for (int i = 0; i < hearts.Count; i++)
        {
            int healthRequiredToShow = Player.instance.maxHealth - i;
            hearts[i].SetActive(health >= healthRequiredToShow);
        }
    }
}