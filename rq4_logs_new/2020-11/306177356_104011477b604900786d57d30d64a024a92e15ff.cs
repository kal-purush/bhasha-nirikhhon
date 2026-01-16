using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class ExcelReader : MonoBehaviour
{
    private TextAsset characterData;

    // Start is called before the first frame update
    void Start()
    {
        characterData = Resources.Load<TextAsset>("Unicornity Character Sheet.xlsx - Sheet1");
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public int GetLevel(string characterID)
    {
        string[] lines = characterData.text.Split(new char[] { '\n' });

        for (int i = 2; i < lines.Length; i++)
        {
            string[] cells = lines[i].Split(new char[] { ',' });

            if (cells[0] == characterID)
            {
                return Convert.ToInt32(cells[1]);
            }
        }
        return 0;
    }
}