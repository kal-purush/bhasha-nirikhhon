using UnityEngine;
using System.Collections;

public abstract class ItemSelector {

    //variable
    protected ObjectPool[] objectPoolList;
    protected int itemNo;

    public ItemSelector() {
        itemNo = GenerateItemList();
    }

    protected abstract int GenerateItemList();

    public GameObject SelectItem() {
        return objectPoolList[Random.Range(0, itemNo)].Retain();
    }
}

public class ChildSelector : ItemSelector {

    protected override int GenerateItemList() {
        int itemNo = 3;
        objectPoolList = new ObjectPool[itemNo];
        objectPoolList[0] = ObjectPool.MakePoolOfObjWithNumber(Resources.Load("Prefab/Children/Basket") as GameObject);
        objectPoolList[1] = ObjectPool.MakePoolOfObjWithNumber(Resources.Load("Prefab/Children/Toiletries") as GameObject);
        objectPoolList[2] = ObjectPool.MakePoolOfObjWithNumber(Resources.Load("Prefab/Children/Ball") as GameObject);
        return itemNo;
    }
}
