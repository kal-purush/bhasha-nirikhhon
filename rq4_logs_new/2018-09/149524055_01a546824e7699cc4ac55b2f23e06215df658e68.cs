using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class UIComboManager : MonoBehaviour
{
    const int sizeChangeSpeed = 3;

    GameObject gameManager;
    GameObject comboNumberObject;
    bool scaleUp = false;
    int combo = 0;

    // Use this for initialization
    void Start()
    {
        gameManager = GameObject.Find("GameManager");
        comboNumberObject = GameObject.Find("TextComboNumber");
    }

    // Update is called once per frame
    void Update()
    {
        if (gameManager)
        {
            if (combo < gameManager.GetComponent<GameManager>().combo)
            {
                scaleUp = true;
            }
            combo = gameManager.GetComponent<GameManager>().combo;
        }

        comboNumberObject.GetComponent<Text>().text = combo.ToString();

        // update text font size
        int size = comboNumberObject.GetComponent<Text>().fontSize;
        if (scaleUp)
        {
            size += sizeChangeSpeed;
            if (size >= 100)
            {
                scaleUp = false;
            }
        }
        else
        {
            size -= sizeChangeSpeed;
            if (size < 60)
            {
                size = 60;
            }
        }
        comboNumberObject.GetComponent<Text>().fontSize = size;
    }
}