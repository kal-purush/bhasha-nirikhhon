using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharacterManager : MonoBehaviour
{
    public Sprite[] FemaleStudent;
    public SpriteRenderer spr;
    private int expNum;

    public void ChangeSprite(string name, string expression)
    {
        if (name == "FemaleStudent1")
        {
            if (expression == "Blush")
            {
                expNum = 0;
            }

            if (expression == "Happy")
            {
                expNum = 1;
            }

            if (expression == "Sad")
            {
                expNum = 2;
            }

            spr.sprite = FemaleStudent[expNum];
        }
    }
}