using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PotTriggerEvent : MonoBehaviour {
    public GameObject cock_tool;
    public GameObject water_tool;
    public GameObject oil_tool;
    public GameObject potcover_tool;

    public GameObject pour_water;
    public GameObject greate_fire;
    public GameObject little_fire;
    public GameObject pour_oil;
    public GameObject rostchick;
    public GameObject potcover_on;

	// Use this for initialization
	void Start () {
        pour_water.SetActive(false);
        pour_oil.SetActive(false);
        rostchick.SetActive(false);
        potcover_on.SetActive(false);

    }
	
	// Update is called once per frame
	void Update () {
		
	}
    private void GetCock()
    {
        rostchick.SetActive(true);
        Destroy(rostchick, 1.5f);
    }
    private void GetWater()
    {
        pour_water.SetActive(true);
        Destroy(pour_water, 1f);
        Invoke("GreateFire", 1.2f);
    }
    private void GreateFire()
    {
        greate_fire.SetActive(true);
        Invoke("GameWin", 2);
    }
    private void GetOil()
    {
        pour_oil.SetActive(true);
        Destroy(pour_oil, 1.5f);
        Destroy(little_fire, 1.5f);
        Invoke("GameFaild", 2.5f);
    }
    private void GetPotcover()
    {
        potcover_on.SetActive(true);
        Invoke("GameFaild", 1.5f);
    }

    private void OnTriggerStay2D(Collider2D collision)
    {
        if (collision.GetComponent<PickupAndDrag>().EventCanTrigger() == false)
            return;
        if(collision.gameObject == cock_tool)
        {
            GetCock();
            Debug.Log("触发成就：炸鸡");
        }
        else if (collision.gameObject == water_tool)
        {
            GetWater();
            Debug.Log("倒水，大火，过关");
        }
        else if (collision.gameObject == oil_tool)
        {
            GetOil();
            Debug.Log("倒油，灭火，失败");
        }
        else if (collision.gameObject == potcover_tool)
        {
            GetPotcover();
            Debug.Log("锅盖，出现，失败");
        }
        Destroy(collision.gameObject);
    }

    private void GameWin()
    {
        GameManager.gm.GameWin();
    }
    private void GameFaild()
    {
        GameManager.gm.GameFaild();
    }
}