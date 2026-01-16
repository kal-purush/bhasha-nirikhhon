using UnityEngine;
using System.Collections;

public class ShopManager : Manager<ShopManager>
{
    [SerializeField]
    private GameObject preShopPopup = null;
    private GameObject shopPopup = null;
    public GameObject ShopPopup
    {
        get
        {
            return shopPopup;
        }
    }

    private int shopType = 0;       // 어떤 제품을 구입할건지
    public int ShopType
    {
        get
        {
            return shopType;
        }
        set
        {
            shopType = value;
        }
    }

	// Use this for initialization
	void Start ()
    {
	    if(preShopPopup == null)
        {
            Debug.LogWarning("The Prefab NOT PREPARED");
        }
    }

    public void OpenShopPopup(int shopType)
    {
        this.shopType = shopType;


    }
}