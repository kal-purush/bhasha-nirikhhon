using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Fever : MonoBehaviour {

    public Slider feverbar;
    public Button btn_fever;
    public float maxFever = 100.0f;

    public void onclick_fever() // 100채우면 피버모드
    {
        feverbar.maxValue = maxFever;

        feverbar.value += 50;
        if (feverbar.value == maxFever)
        {
            PlayerData.getInstance().fever = true;
            btn_fever.interactable = false;
            while(feverbar.value != 0.0f)
                feverMode();
            Debug.Log(PlayerData.getInstance().fever);
        }

    }



    public void feverMode() // 피버모드 발동
    {

        feverbar.value -= Time.deltaTime * 100 * 0.5f;
        Debug.Log(feverbar.value);

        if (feverbar.value == 0.0f)
        {
            PlayerData.getInstance().fever = false;
            btn_fever.interactable = true;
        }
    }
}