using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
public class CharacterMenu : MonoBehaviour
{
    //TextFields
    public Text levelText, goldText, xpText;

    //logic
    public Image character;
    public Image weapon;
    public RectTransform xpBar;


    public void UpdateMenu()
    {
        //Gold
        goldText.text = "DISPLAY GOLD";

        //Xp Bar
        xpText.text = "DISPLAY XP AMMOUNT";
        xpBar.localScale = new Vector3(0.5f, 0, 0);

        //level
        levelText.text = "DISPLAY LEVEL"; 
    }

   
}