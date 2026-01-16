using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class N_CardSystem : MonoBehaviour
{

    public bool isGame = true;
    public int GameMinute = 5;
    public Slider HeroSlider;
    public Animator HeroAnimator;

    // Start is called before the first frame update
    void Awake()
    {
        HeroSlider.maxValue = GameMinute * 120;
        StartCoroutine("HeroTimer");
    }

    IEnumerator HeroTimer()
    {
        print("start");
        while (isGame)
        {
            if (HeroSlider.value <= 0)
            {
                isGame = false;
            }
            HeroSlider.value -= 1;
            yield return new WaitForSecondsRealtime(0.5f);
        }
    }

}