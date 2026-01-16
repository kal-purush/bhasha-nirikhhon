using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SliderController : MonoBehaviour
{
    private GameManager gameManager;
    //Sliderを入れる
    public Slider slider;

    //HP(MAX)
    int maxHp = 155;
    //現在のHP
    int currentHp;

    // Start is called before the first frame update
    void Start()
    {
        gameManager = GameObject.FindGameObjectWithTag("GameController").GetComponent<GameManager>();
        //Sliderを満タンにする。
        slider.value = 1;
        //現在のHPを最大HPと同じに。
        currentHp = maxHp;
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    //体力ゲージ減少の処理
    public void crush(int damage)
    {
        //現在のHPからダメージを引く
        currentHp = currentHp - damage;
        //現在のHPをSliderに反映。
        slider.value = (float)currentHp / (float)maxHp;
        if (currentHp < 0)
        {
            gameManager.Dead();
        }
    }
}