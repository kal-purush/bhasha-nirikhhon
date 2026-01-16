using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
//Controls the alcolol meter for each player :)
public class AlcololMeter : MonoBehaviour
{
    private bool init;
    private Image[] beerImages;
    
    void Start()
    {
        Init();
    }

    void Init()
    {
        if (init)
            return;
        
        beerImages = GetComponentsInChildren<Image>();
        init = true;
    }

    public void SetAlcololAmount(int amount)
    {
        Init();
        
        for (int i = 0; i < beerImages.Length; i++)
        {
            if (i < amount)
            {
                beerImages[i].enabled = true;
            }
            else
            {
                beerImages[i].enabled = false;
            }
        }
    }
}