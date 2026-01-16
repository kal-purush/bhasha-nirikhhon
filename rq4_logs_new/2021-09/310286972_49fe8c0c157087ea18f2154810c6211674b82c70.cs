using System.Collections;
using System.Collections.Generic;
using System.IO;
using UnityEngine;

public class SaveDataController : MonoBehaviour
{
    private TextAsset csvFile;
    private List<string[]> saveData = new List<string[]>();

    private void Start()
    {
        LoadData();
    }

    public void LoadData()
    {
        csvFile = Resources.Load("testCSV") as TextAsset; // Resouces下のCSV読み込み
        StringReader reader = new StringReader(csvFile.text);
        while (reader.Peek() != -1) // reader.Peaekが-1になるまで
        {
            string line = reader.ReadLine(); // 一行ずつ読み込み
            saveData.Add(line.Split(',')); // , 区切りでリストに追加
        }

        // csvDatas[行][列]を指定して値を自由に取り出せる
        Debug.Log(saveData[0][1]);
    }

    public void WriteData()
    {

    }
}