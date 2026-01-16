using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Codice.Client.BaseCommands.Import;
using UnityEngine;
using TMPro;

[Serializable]
public class SaveData
{
    public int Money = 0;
}


public class BankUIController : MonoBehaviour
{
    
    public static event Action<int> OnMoneyChange; 

    public TextMeshProUGUI BankText;
    public static BankUIController Instance;
    
    private const string MoneyText = "Money: $";
    public SaveData MoneyData;


    private void Awake()
    {

        Instance = this;
        OnMoneyChange += UpdateMoney;
        if (MoneyData.Money == 0)
        {
            MoneyData.Money = 200;
        }
        string MoneyJson = File.ReadAllText(Application.dataPath + "/save.json");
        SaveData NewMoney = JsonUtility.FromJson<SaveData>(MoneyJson);
        MoneyData = NewMoney;

    }

    private void OnDestroy()
    {
        OnMoneyChange -= UpdateMoney;
    }

    public void UpdateMoney(int money)
    {
        MoneyData.Money += money;

        string jsonString = JsonUtility.ToJson(MoneyData);
        
        File.WriteAllText(Application.dataPath + "/save.json", jsonString);
        
        BankText.SetText(MoneyText + MoneyData.Money.ToString());
    }


}