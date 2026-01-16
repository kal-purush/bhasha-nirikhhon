using UnityEngine;
using System.Collections;

public class SaveAndLoad : MonoBehaviour {

    string strItemBuy = "itemBuy";
    string strItemLock = "itemLock";

    string strStageBuy = "stageBuy";
    string strStageLock = "stageLock";

    string strChapBuy = "chapBuy";
    string strChapLock = "chapLock";

    string strCoin = "Coin";
    string strDia = "Dia";

    string strHitPoint = "HitPoint";

    public void SaveDB()
    {
        string itemBuy = "";
        string itemLock = "";

        // 아이템 정보 저장
        for (int i = 0; i < GameDataManager.Instance.itemData.itemProperty.Length; i++)
        {
            if (i < GameDataManager.Instance.itemData.itemProperty.Length - 1)
            {
                itemBuy += GameDataManager.Instance.itemData.itemProperty[i].isBuy.ToString() + ",";
                itemLock += GameDataManager.Instance.itemData.itemProperty[i].isLock.ToString() + ",";
            }
            else
            {
                itemBuy += GameDataManager.Instance.itemData.itemProperty[i].isBuy.ToString();
                itemLock += GameDataManager.Instance.itemData.itemProperty[i].isLock.ToString();
            }
        }
        PlayerPrefs.SetString(strItemBuy, itemBuy);
        PlayerPrefs.SetString(strItemLock, itemLock);



        itemBuy = "";
        itemLock = "";
        // 스테이지 정보 저장
        for (int i = 0; i < GameDataManager.Instance.itemData.stageProperty.Length; i++)
        {
            if (i < GameDataManager.Instance.itemData.stageProperty.Length - 1)
            {
                itemBuy += GameDataManager.Instance.itemData.stageProperty[i].isBuy.ToString() + ",";
                itemLock += GameDataManager.Instance.itemData.stageProperty[i].isLock.ToString() + ",";
            }
            else
            {
                itemBuy += GameDataManager.Instance.itemData.stageProperty[i].isBuy.ToString();
                itemLock += GameDataManager.Instance.itemData.stageProperty[i].isLock.ToString();
            }
        }
        PlayerPrefs.SetString(strStageBuy, itemBuy);
        PlayerPrefs.SetString(strStageLock, itemLock);



        itemBuy = "";
        itemLock = "";
        // 챕터 정보 저장
        for (int i = 0; i < GameDataManager.Instance.itemData.chapProperty.Length; i++)
        {
            if (i < GameDataManager.Instance.itemData.chapProperty.Length - 1)
            {
                itemBuy += GameDataManager.Instance.itemData.chapProperty[i].isBuy.ToString() + ",";
                itemLock += GameDataManager.Instance.itemData.chapProperty[i].isLock.ToString() + ",";
            }
            else
            {
                itemBuy += GameDataManager.Instance.itemData.chapProperty[i].isBuy.ToString();
                itemLock += GameDataManager.Instance.itemData.chapProperty[i].isLock.ToString();
            }
        }
        PlayerPrefs.SetString(strChapBuy, itemBuy);
        PlayerPrefs.SetString(strChapLock, itemLock);


        // 유저 정보
        int coin = GameDataManager.Instance.userData.currentCoin;
        int dia = GameDataManager.Instance.userData.currentDia;

        PlayerPrefs.SetInt(strCoin, coin);
        PlayerPrefs.SetInt(strDia, dia);

        string hitPoint = "";
        for (int i = 0; i < GameDataManager.Instance.userData.hitPoint.Length; i++)
        {
            if (i < GameDataManager.Instance.userData.hitPoint.Length - 1)
                hitPoint += GameDataManager.Instance.userData.hitPoint[i].ToString() + ",";
            else
                hitPoint += GameDataManager.Instance.userData.hitPoint[i].ToString();
        }


    }
    public void LoadDB()
    {
        // 아이템 로드
        if (PlayerPrefs.GetString(strItemBuy) != "")
        {
            string[] itemBuy;
            string[] itemLock;

            itemBuy = PlayerPrefs.GetString(strItemBuy).Split(',');
            itemLock = PlayerPrefs.GetString(strItemLock).Split(',');

            for (int i = 0; i < GameDataManager.Instance.itemData.itemProperty.Length; i++)
            {
                GameDataManager.Instance.itemData.itemProperty[i].isBuy = System.Convert.ToBoolean(itemBuy[i]);
                GameDataManager.Instance.itemData.itemProperty[i].isLock = System.Convert.ToBoolean(itemLock[i]);
            }
        }

        // 스테이지 로드
        if (PlayerPrefs.GetString(strStageBuy) != "")
        {
            string[] stageBuy;
            string[] stageLock;

            stageBuy = PlayerPrefs.GetString(strStageBuy).Split(',');
            stageLock = PlayerPrefs.GetString(strStageLock).Split(',');

            for (int i = 0; i < GameDataManager.Instance.itemData.stageProperty.Length; i++)
            {
                GameDataManager.Instance.itemData.stageProperty[i].isBuy = System.Convert.ToBoolean(stageBuy[i]);
                GameDataManager.Instance.itemData.stageProperty[i].isLock = System.Convert.ToBoolean(stageLock[i]);
            }
        }

        // 챕터 로드
        if (PlayerPrefs.GetString(strChapBuy) != "")
        {
            string[] chapBuy;
            string[] chapLock;

            chapBuy = PlayerPrefs.GetString(strChapBuy).Split(',');
            chapLock = PlayerPrefs.GetString(strChapLock).Split(',');

            for (int i = 0; i < GameDataManager.Instance.itemData.chapProperty.Length; i++)
            {
                GameDataManager.Instance.itemData.chapProperty[i].isBuy = System.Convert.ToBoolean(chapBuy[i]);
                GameDataManager.Instance.itemData.chapProperty[i].isLock = System.Convert.ToBoolean(chapLock[i]);
            }
        }



        // 유저 정보 로드
        GameDataManager.Instance.userData.currentCoin = PlayerPrefs.GetInt(strCoin);
        GameDataManager.Instance.userData.currentDia = PlayerPrefs.GetInt(strDia);

        
        if (PlayerPrefs.GetString(strHitPoint) != "")
        {
            string[] hitPoint;

            hitPoint = PlayerPrefs.GetString(strHitPoint).Split(',');        

            for (int i = 0; i < GameDataManager.Instance.userData.hitPoint.Length; i++)
            {
                GameDataManager.Instance.userData.hitPoint[i] = System.Convert.ToInt32(hitPoint[i]);
            }
        }


    }
}