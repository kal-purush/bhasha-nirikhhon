using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
//Controls the alcolol meter for each player :)
public class AlcololMeter : MonoBehaviour
{
    private bool init;
    private Image[] beerImages;
    public Sprite emptySprite, fullSprite;
    
    void Start()
    {
        Init();
    }

    void Init()
    {
        if (init)
            return;
        
        //get the bottle images
        beerImages = GetComponentsInChildren<Image>();
        
        //disable all to start
        DisableAllBottles();
        
        init = true;
    }
    
    void EnableAllBottles()
    {
        for (int i = 0; i < beerImages.Length; i++)
        {
            beerImages[i].enabled = true;
        }
    }

    void DisableAllBottles()
    {
        for (int i = 0; i < beerImages.Length; i++)
        {
            beerImages[i].enabled = false;
        }
    }

    public void SetAlcololAmount(int amount)
    {
        Init();
        
        for (int i = 0; i < beerImages.Length; i++)
        {
            //is it within the specified amount?
            if (i < amount)
            {
                //enable it and set to full sprite
                beerImages[i].enabled = true;
                SetSprite(beerImages[i], fullSprite);
            }
            //just set it to empty sprite then!
            else
            {
                SetSprite(beerImages[i], emptySprite);
            }
        }
    }

    public void SetSprite(Image image, Sprite newSprite)
    {
        image.sprite = newSprite;
    }
}