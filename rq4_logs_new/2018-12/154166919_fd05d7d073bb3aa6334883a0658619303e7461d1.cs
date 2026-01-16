using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class ClickToShakeChopstick : MonoBehaviour {
    public GameObject ChopstickWithMagic;
    public GameObject ChopstickWithoutMagic;
    public GameObject Mask;
    public GameObject ChopsticksOnCupboard;
    public GameObject ChopsticksOnCupboardWithMagic;
    public GameObject ChopsticksOnCupboardWithoueMagic;
    public GameObject chopstick;
    public GameObject magicstick;
    public GameObject notice;

    private GameObject ChopstickNow;
    private bool shaking;
    private int ShakeTimes = 0;
    private bool GotMagicstick;
    private bool GotChopstick;
    private int ChopsticksNum; //成就

    private float ClickInterval;
	// Use this for initialization
	void Start () {
        ChopstickNow = ChopstickWithMagic;
        HideMask();
        shaking = false;
        GotMagicstick = false;
        ChopsticksNum = 0;
        ClickInterval = 0;
        ChangeChopsticks(!GotMagicstick);
	}
	
	// Update is called once per frame
	void Update () {
        ClickInterval += Time.deltaTime;
	}
    private void ChangeChopsticks(bool withmagic)
    {
        ChopstickWithMagic.SetActive(withmagic);
        ChopstickWithoutMagic.SetActive(!withmagic);
        ChopsticksOnCupboardWithMagic.SetActive(withmagic);
        ChopsticksOnCupboardWithoueMagic.SetActive(!withmagic);
    }
    private void HideMask()
    {
        Mask.SetActive(false);
    }
    
    private void Shake()
    {
        Mask.SetActive(true);
        shaking = true; //false -> true;
        ChopstickNow.GetComponent<ShakeChopstick>().Shake();
    }
    private void GetTool()
    {
        ShakeTimes++;
        Invoke("HideMask", 0.5f);
        shaking = false; //true -> false;
        ChopstickNow.GetComponent<ShakeChopstick>().StopShake();
        if (ChopstickNow.GetComponent<ShakeChopstick>().GetTool(ShakeTimes))//获得魔杖
        {
            GotMagicstick = true;
            ChangeChopsticks(!GotMagicstick);
            ChopstickNow = ChopstickWithoutMagic;
            ChopsticksNum = 0;
            //提示
            notice.SetActive(true);
            notice.GetComponent<GetToolsNotice>().ChangeText("摇出了一根魔杖");
            //拾取魔杖
            magicstick.SetActive(true);
            magicstick.GetComponent<PickupAndDrag>().PickByOtherScript();
            //成就：欧皇
            if (ShakeTimes == 1)
                Debug.Log("成就达成：欧皇");
        }
        else//获得筷子
        {
            //提示
            notice.SetActive(true);
            notice.GetComponent<GetToolsNotice>().ChangeText("摇出了一根筷子");
            //拾取筷子
            if (!GotChopstick)
            {
                chopstick.SetActive(true);
                chopstick.GetComponent<PickupAndDrag>().PickByOtherScript();
            }
            GotChopstick = true;
            ChopsticksNum++;
            if (ChopsticksNum >= 3)
            {
                Debug.Log("成就达成: 非酋");
                ChopsticksNum = 0;
            }
        }
    }
    private void OnMouseDown()
    {
        if (ClickInterval <= 0.8f)
            return;
        else
            ClickInterval = 0;

        if (ShakeTimes >= 5)
        {
            ChopsticksOnCupboard.SetActive(true);
            Destroy(ChopsticksOnCupboard.GetComponent<ShowSthOnClick>());
            this.gameObject.SetActive(false);
            return;
        }
        if (!shaking)
            Shake();
        else
            GetTool();
    }
}