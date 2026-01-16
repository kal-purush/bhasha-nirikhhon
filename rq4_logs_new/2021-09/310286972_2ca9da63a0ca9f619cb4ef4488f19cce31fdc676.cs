using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class DivideBar : MonoBehaviour
{
    public Slider sli;
    public int hp, skip, heal, maxHP;
    public float nowHP, percent, atai;
    public bool judge1, judge2;

    void Start()
    {
        heal = hp;
        judge1 = false;
        judge2 = false;
        nowHP = maxHP;
    }

    // Update is called once per frame
    void Update()
    {
        sli.value = hp;
        if (Input.GetKeyDown(KeyCode.B))
        {
            hp -= 25;
            nowHP -= 25;
            if (hp == 120 || hp == 60)
            {
                judge1 = true;
            }
        }
        if (Input.GetKeyDown(KeyCode.V))
        {
            if (hp == 110 || hp == 50)
            {
                judge2 = true;
            }
            hp += 1;
            nowHP += 1;
        }

        if (judge1)
        {
            hp -= 10;
            judge1 = false;
        }
        if (judge2)
        {
            hp += 10;
            judge2 = false;
        }

        if (Input.GetKeyDown(KeyCode.C))
        {
            hp = heal;
            nowHP = maxHP;
        }

    }
}