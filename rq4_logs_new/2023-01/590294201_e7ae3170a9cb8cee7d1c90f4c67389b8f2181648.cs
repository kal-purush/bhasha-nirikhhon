using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ADMarchentScript : MonoBehaviour
{
    /********************************** 싱 글 톤 *******************************************/
    //private static GameManager g_Manager = null;
    public static ADMarchentScript Instance
    {
        get
        {
            if (instance == null)
            {
                instance = FindObjectOfType(typeof(ADMarchentScript)) as ADMarchentScript;
                if (Instance == null)
                {
                    GameObject obj = new GameObject("GameManager");
                    instance = obj.AddComponent(typeof(ADMarchentScript)) as ADMarchentScript;
                }
            }
            return instance;
        }
    }
    private static ADMarchentScript instance;

    /*************************************************************************************/

    MyObject myChar;
    private Animator _anim;

    private int DropCountCheck;
    private int DropCnt;

    public GameObject Gold;
    public int GoldCnt;

    void Start()
    {
        myChar = MyObject.MyChar;
        _anim = GetComponent<Animator>();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void ADUse()
    {
        _anim.SetBool("ADUse", true);
        GoldCnt = 10;
    }
    public void Itemdrop()
    {
        DropTemplateMgr Drop = GameManager.Instance.DropDataMgr;

        //int RanCntSlotCoin = Random.Range(Drop.GetTemplate(0).HeroMin, Drop.GetTemplate(0).HeroMax);
        //for (int i = 0; i < RanCntSlotCoin; i++)
        //{
        //    StartCoroutine(ItemIns(SlotCoin, i * 0.1f));
        //}

        //int RanCntGold = Random.Range(Drop.GetTemplate(1).GiftMin, Drop.GetTemplate(1).GiftMax);
        for (int i = 0; i < 10; i++)
        {
            StartCoroutine(ItemIns(Gold, (i + GoldCnt) * 0.1f));
        }

        //int RanCntGem = Random.Range(Drop.GetTemplate(2).GiftMin, Drop.GetTemplate(2).GiftMax);
        //for (int i = 0; i < RanCntGem; i++)
        //{
        //    StartCoroutine(ItemIns(Gem, (i + RanCntGem) * 0.1f));
        //}

        //int RanCntHeart = Random.Range(Drop.GetTemplate(3).GiftMin, Drop.GetTemplate(3).GiftMax);
        //for (int i = 0; i < RanCntHeart; i++)
        //{
        //    StartCoroutine(ItemIns(Heart, i * 0.1f));
        //}

        //int RanCntShield = Random.Range(Drop.GetTemplate(4).GiftMin, Drop.GetTemplate(4).GiftMax);
        //for (int i = 0; i < RanCntShield; i++)
        //{
        //    StartCoroutine(ItemIns(Shield, i * 0.1f));
        //}

        //int RanCntHero = Random.Range(Drop.GetTemplate(7).GiftMin, Drop.GetTemplate(7).GiftMax);
        //for (int i = 0; i < RanCntHero; i++)
        //{
        //    StartCoroutine(ItemIns(HeroHeart, i * 0.1f));
        //}

        //int RanCntSoulSpark = Random.Range(Drop.GetTemplate(6).GiftMin, Drop.GetTemplate(6).GiftMax);
        //for (int i = 0; i < RanCntSoulSpark; i++)
        //{
        //    StartCoroutine(ItemIns(SoulSpark, (i + 1f + RanCntGold + RanCntGem) * 0.1f));
        //    if (i == RanCntSoulSpark - 1)
        //    {
        //        LastCheck = true;
        //    }
        //}
        DropCountCheck = GoldCnt;
    }

    private void ItemInstanti(GameObject Object)
    {
        GameObject Item = Instantiate(Object, transform.position, Quaternion.identity);
        //Item.GetComponent<GiftBox_ItemScript>().StartPostion = gameObject.transform.position;
        Item.transform.parent = transform.parent;
        Item.transform.localScale = new Vector2(1, 1);

        float xCnt = Random.Range(-0.5f, 0.5f);
        float yCnt = Random.Range(0.0f, 0.1f);
        Item.GetComponent<Rigidbody2D>().AddForce(new Vector2(xCnt, yCnt) * 100);
        Item.GetComponent<Rigidbody2D>().constraints = RigidbodyConstraints2D.None;
        Item.GetComponent<Rigidbody2D>().constraints = RigidbodyConstraints2D.FreezeRotation;
    }
    IEnumerator ItemIns(GameObject Object, float TimeCnt)
    {
        yield return new WaitForSeconds(TimeCnt);
        ItemInstanti(Object);
        GameManager.Instance.SFXSound(30);
        DropCnt++;
        if (DropCountCheck == DropCnt)
        {
            StartCoroutine(PickUpTrue());
        }
    }
    IEnumerator PickUpTrue()
    {
        yield return new WaitForSeconds(1.5f);
        myChar.GiftboxDropCheck = true;
    }
}