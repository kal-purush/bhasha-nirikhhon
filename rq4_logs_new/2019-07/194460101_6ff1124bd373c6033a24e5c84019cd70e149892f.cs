using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HpEnemy : MonoBehaviour
{
    public Slider hpSlider;
    private void Update()
    {
        float hpTotal;
        if(gameObject.name == "Enemy2")
        {
            hpTotal = 200;
        }
        else
        {
            hpTotal = 100;
        }
        hpSlider.value = transform.gameObject.GetComponent<Enemy>().hp / hpTotal ;
    }
}