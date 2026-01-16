using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class UILifeManager : MonoBehaviour
{
    const int initLife = 3;

    int life = initLife;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        // test
        if (Input.GetKeyDown(KeyCode.Alpha1))
        {
            LifeUp();
        }
        if (Input.GetKeyDown(KeyCode.Alpha2))
        {
            LifeDown();
        }

        for (int i = 1; i <= initLife; ++i)
        {
            GameObject obj = GameObject.Find("ImageLife" + i.ToString());
            if (i > life)
            {
                Color color = obj.GetComponent<Image>().color;
                color.a -= 0.1f;
                if(color.a < 0)
                {
                    color.a = 0;
                }
                obj.GetComponent<Image>().color = color;
            }
            else
            {
                Color color = obj.GetComponent<Image>().color;
                color.a += 0.1f;
                if (color.a > 1.0f)
                {
                    color.a = 1.0f;
                }
                obj.GetComponent<Image>().color = color;
            }
        }
    }

    public bool LifeUp()
    {
        if (life < initLife)
        {
            ++life;
            return true;
        }
        return false;
    }

    public bool LifeDown()
    {
        if (life > 0)
        {
            --life;
            return true;
        }
        return false;
    }
}