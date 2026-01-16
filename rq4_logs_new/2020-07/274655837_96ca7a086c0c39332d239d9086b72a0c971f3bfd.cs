using BaseStructures;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

[System.Serializable]
public class ShopData
{
    public List<Pair<string,bool>> checkers = new List<Pair<string, bool>>();
}

public class Shop : MonoBehaviour
{
    public static GameObject PlayerMoneyText;
    public GameObject currentPuck;
    PlayerData playerData; 
    ShopData shopData; //данные об шайбах в магазине

    public GameObject shopCheckers; //обьект в котором находятся шайбы, которые можно купить

    void Start()
    {
        shopData = new ShopData();
        XMLManager.LoadShop(ref shopData);

        for(int i = 0; i < shopCheckers.transform.childCount && i < shopData.checkers.Count;i++)
                shopCheckers.transform.GetChild(i+3).gameObject.transform.GetComponent<Image>().sprite = Resources.Load<Sprite>("Sprites/levels/checkers/" + shopData.checkers[i].first);

        playerData = PlayerData.getInstance();
        currentPuck.GetComponent<Image>().sprite = Resources.Load<Sprite>("Sprites/levels/checkers/" + playerData.puckSprite);

        PlayerMoneyText = this.transform.GetChild(2).transform.GetChild(2).gameObject;
        LoadMoney();
    }

    public static void LoadMoney()
    {
        PlayerData playerData = PlayerData.getInstance();
        XMLManager.LoadData<PlayerData>(ref playerData, "PlayerData");
        PlayerMoneyText.GetComponent<Text>().text = playerData.money.ToString();
    }

    public void BuyPuck(GameObject puck)
    {
        int money = int.Parse(puck.transform.GetChild(0).transform.GetComponent<Text>().text);
        if (playerData.money >= money)
        {
            Sprite puckSprite = puck.GetComponent<Image>().sprite;
            currentPuck.GetComponent<Image>().sprite = puckSprite;
            puck.transform.GetChild(0).gameObject.SetActive(false);

            playerData.puckSprite = puckSprite.name;
            playerData.money -= money;
            playerData.Save();

            LoadMoney();
        }
        else
        {
            Debug.Log("Not enough money to buy!");
        }


       
    }
}