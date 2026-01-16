using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class doorhp : MonoBehaviour
{
    public int damage;
    public int maxHp = 155;
    private int currentHp;
    public Slider slider;
    public GameObject uipanel;

    public doorscore door;

    // Start is called before the first frame update
    void Start()
    {
     
        slider.value = 1;
        currentHp = maxHp;
        uipanel.SetActive(false);

        
    }

    // Update is called once per frame
    void FixedUpdate()
    {

        slider.value = (float)currentHp / (float)maxHp;

        if (door.hit == true)
        {
            uipanel.SetActive(true);
        }

        if (door.nowanim == 1)
        {
            if (currentHp >= 220)
            {
                currentHp = currentHp - 2;
            }
        }

        if (door.nowanim == 2)
        {
            if (currentHp >= 180)
            {
                currentHp = currentHp - 2;
            }
        }

        if (door.nowanim == 3)
        {
            if (currentHp >= 120)
            {
                currentHp = currentHp - 2;
            }
        }

        if (door.nowanim == 4)
        {
            if (currentHp >= 60)
            {
                currentHp = currentHp - 2;
            }
        }

        if (door.nowanim == 5)
        {
            if (currentHp >= 0)
            {
                currentHp = currentHp - 2;
            }
        }

        if (door.nowanim == 6)
        {
            uipanel.SetActive(false);
        }

    }
}