using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class FoodList : MonoBehaviour
{
    FoodStatusManager2 foodStatusManager;

    List<FoodStatusManager2> foodList = new List<FoodStatusManager2>();

    // ここでファイルが入っているフォルダ場所を指定(適宜変えてください。)
    // 絶対パスの場合は一番後ろの + "\\" は消さないでください。
    // Application.dataPathを使う場合は消して大丈夫です。
    string directory = "C:/Users/vantan/Desktop/作業用ZooGame/Assets/hujiwara" + "/";
    // 読み込むファイル名(csv専用です。)
    string path = "FoodStatus.csv";

    bool isCreate = false;

    CSVReader reader = new CSVReader();

    void Awake()
    {
        reader.Load(Application.dataPath + "/" + path);
    }

    void Start()
    {   
        if(!isCreate)
        {
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }

        for(int i = 0; i < 6; ++i)
        {
            foodList.Add(new FoodStatusManager2());
        }

        for (int i = 0; i < 6; ++i)
        {
            foodList[i].ID = reader.GetInt(i, 0);
            foodList[i].Name = reader.GetString(i, 1);
            foodList[i].purchasePrice = reader.GetInt(i, 2);
            foodList[i].loveDegreeUpValue = reader.GetFloat(i, 3);
            foodList[i].satietyLevelUpValue = reader.GetFloat(i, 4);
            foodList[i].foodType = reader.GetInt(i, 5);
            foodList[i].possessionNumber = reader.GetInt(i, 6);
        }
    }

}