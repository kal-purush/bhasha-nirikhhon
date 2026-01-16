using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using UnityEngine;

// Этот класс должен храниь все данные уровня (сложность ИИ, тип поля, режим и т.д.)
public class Level : MonoBehaviour
{
    public int NumberPlanet, NumberLvl;

    public void OnEnable()
    {
        if (!PlayerData.getInstance().progress[NumberPlanet][NumberLvl])
        {
            Color32 thisColor = gameObject.GetComponent<Image>().color; 
            gameObject.GetComponent<Image>().color = new Color32(thisColor.r, thisColor.g, thisColor.b, 170);
        }
    }

}