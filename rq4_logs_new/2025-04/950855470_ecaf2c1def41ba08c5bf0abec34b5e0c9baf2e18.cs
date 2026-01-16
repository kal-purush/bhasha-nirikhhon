using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CombatHud : MonoBehaviour
{
    public Text nameText;
    public Text healthText;
    public Slider hpSlider;

    public void setHUD(CombatEntity entity)
    {
        nameText.text = entity.GetName();

        int maxhp = entity.GetMaxHealth();
        int hp = entity.GetHealth();

        hpSlider.maxValue = maxhp;
        hpSlider.value = hp;

        healthText.text = "HP: " + hp + "/" + maxhp;
    }

    public void setHP(int hp)
    {
        hpSlider.value = hp;
    }

}