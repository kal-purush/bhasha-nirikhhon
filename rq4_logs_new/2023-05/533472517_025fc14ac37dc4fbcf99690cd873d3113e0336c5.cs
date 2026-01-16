using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FactData : MonoBehaviour
{
    public TextAsset factSheet; //The CSV file
    public string[] FactList; //The list of subclass planets used to store the data

    // Start is called before the first frame update
    void Start()
    {
        FactList = new string[32];
        ReadCSV();
    }

    void ReadCSV()
    {
        string[] data = factSheet.text.Split(new string[] { "\n" }, System.StringSplitOptions.None);
        for (int i = 0; i < 32; i++)
        {
            
            FactList[i] = data[i];
        }
    }
}