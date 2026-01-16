using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HP_ZERO : MonoBehaviour
{
    /*
        <<HPがゼロになった時の遷移に使うとき>>
    
        1.playerのHPがゼロになったとき

            public HP_ZERO hp_zero;

                と宣言しておいて

            if(!hp_zero.playerHP_ZERO)
            {
            }

        2.bossのHPがゼロになったとき

            public HP_ZERO hp_zero;

                と宣言しておいて

            if(!hp_zero.bossHP)
            {
            }
    */
    
    //
    public Slider playerHP;
    public Slider bossHP;

    public static bool playerHP_ZERO = false;
    public static bool bossHP_ZERO = false;

    void Update()
    {
        if(playerHP.value == 0)
        {
            playerHP_ZERO = true;
        }
        if(playerHP.value == 0)
        {
            bossHP_ZERO = true;
        }
    }
}