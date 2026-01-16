using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;


public class HeartInfo
{
    public int num;
    public string name;
    public string effect;
    public int price;
    public float benefit;
}

public class Heart : MonoBehaviour {

    private static Heart instance = null;

    public GameObject heart_Prefab;
    public GameObject parent_obj;

    public List<HeartInfo> heartList;

    private Heart()
    {
        heartList = new List<HeartInfo>();
        addList();

    }

    public static Heart getInstance()
    {
        if (instance == null)
            instance = new Heart();

        return instance;
    }

    public void Start()
    {
        create_Item(heart_Prefab,heartList);
        
    }

    public void create_Item(GameObject prefab, List<HeartInfo> list)
    {
        GameObject item;

        for (int i = 0; i < list.Count; i++)
        {
            item = Instantiate(prefab, parent_obj.transform); // 생성
            item.transform.localScale = new Vector3(1, 1, 1); // 스케일 설정
            item.GetComponent<HeartItem>().num = list[i].num;
            item.GetComponent<HeartItem>().name = list[i].name;
            item.GetComponent<HeartItem>().effect = list[i].effect;
            item.GetComponent<HeartItem>().price = list[i].price;
            item.GetComponent<HeartItem>().benefit = list[i].benefit;

            item.GetComponent<HeartItem>().Name.text = list[i].name;
            item.GetComponent<HeartItem>().Effect.text = list[i].effect;
            item.GetComponent<HeartItem>().Price.text = ((int)list[i].price).ToString();
        }

    }

    public void addList()
    {
        addHeart(1, "길고양이,길강아지","피버동안 돌멩이 10개",   0, 10.0f);
        addHeart(2, "할머니, 할아버지", "피버동안 돌멩이 20개", 100, 20.0f);
        addHeart(3, "아저씨, 아줌마", "피버동안 돌멩이 30개", 300, 30.0f);
        addHeart(4, "중고생", "피버동안 돌멩이 40개", 1000, 40.0f);
        addHeart(5, "소녀팬", "피버동안 돌멩이 50개", 5000, 501.0f);

    }

    public void addHeart(int num, string name, string effect, int price, float benefit)
    {
        HeartInfo Heart_ = new HeartInfo();

        Heart_.num = num;
        Heart_.name = name;
        Heart_.effect = effect;
        Heart_.price = price;
        Heart_.benefit = benefit;

        heartList.Add(Heart_);

    }


}