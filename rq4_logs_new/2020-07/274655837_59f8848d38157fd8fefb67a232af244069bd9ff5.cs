using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class bonuses : MonoBehaviour
{
    public GameObject Days; //массив дней

    private void Start()
    {
        for (int i = 1; i <= Days.transform.childCount; ++i)
        {
            Days.transform.GetChild(i - 1).GetChild(1).GetComponent<Text>().text += " " + i.ToString();

            //установка текста бонусов, сам бонус устанавливается в виде тега на кнопку
            Text text = Days.transform.GetChild(i - 1).GetChild(4).GetComponent<Text>();
            Transform tag = Days.transform.GetChild(i - 1);

            //установка бонусов  
            switch (i)
            {
                case 1:
                    text.text = "+100 " + text.text;
                    tag.tag = "100";
                    break;
                case 2: 
                    text.text = "+250 " + text.text;
                    tag.tag = "250";
                    break;
                case 3:
                    text.text = "+5 " + text.text;
                    tag.tag = "reductor";
                    break;
                case 4:
                    text.text = "+500 " + text.text;
                    tag.tag = "500";
                    break;
                case 5: 
                    text.text = "+5 " + text.text;
                    tag.tag = "booster";
                    break;
                case 6: 
                    text.text = "+500 " + text.text;
                    tag.tag = "500";
                    break;
                case 7: 
                    text.text = "+500 " + text.text;
                    tag.tag = "500";
                    break;
                case 8: 
                    text.text = "+1000 " + text.text;
                    tag.tag = "1000";
                    break;
                case 9: 
                    text.text = "+1000 " + text.text;
                    tag.tag = "1000";
                    break;
            }
        }
    }
}