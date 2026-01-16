using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BackgroundManager : MonoBehaviour
{
    public Sprite[] backgrounds;
    public SpriteRenderer backGroundSpr;

    // Start is called before the first frame update
    void Start()
    {
        //Start background goes here
    }

    public void BackgroundChange(string bgName)
    {
        bool bgFound = false;

        for(int i = 0; i < backgrounds.Length; i++)
        {
            if (bgName == backgrounds[i].name)
            {
                backGroundSpr.sprite = backgrounds[i];
                bgFound = true;
            }
        }

        if (!bgFound)
        {
            Debug.Log("Background not found!");
        }
    }

}