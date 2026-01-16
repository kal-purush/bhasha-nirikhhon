using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class BattleHud : MonoBehaviour
{
    public Text nameText;
    public Text healthText;
    public Slider hpSlider;

    public void setHUD(Unit unit)
    {
        nameText.text = unit.unitName;
        hpSlider.maxValue = unit.maxHp;
        hpSlider.value = unit.currentHp;

        healthText.text = "HP: " + unit.currentHp + "/" + unit.maxHp;
    }

    public void setHP(int hp)
    {
        hpSlider.value = hp;
    }

}