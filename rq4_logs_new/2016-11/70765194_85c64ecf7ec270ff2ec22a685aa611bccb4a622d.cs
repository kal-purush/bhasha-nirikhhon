using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class DebugShopAnimalList : MonoBehaviour
{
    // GetComponentするためのGameObject
    public GameObject animalStatus;
    // AnimalStatusスクリプトのリスト
    public List<AnimalStatusManager> animalList = new List<AnimalStatusManager>();

    // 一応Awakeで初期化
    void Awake()
    {
        Init();
    }

    void Init()
    {
        // AnimalStatusスクリプト
        var animal = animalStatus.GetComponent<AnimalStatusManager>();
        
        // リスト追加
        for(int i = 0; i < 5; ++i)
        {
            animalList.Add(Instantiate(animal));
        }
        
        // AnimalStatusを初期化(仮)
        for(int i = 0; i < 5; ++i)
        {
            animalList[i].status.ID = i;
            animalList[i].status.Rarity = i;
            animalList[i].status.IsPurchase = false;
        }

    }
}