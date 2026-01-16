using UnityEngine;
using UnityEngine.UI;
using System.Collections;

[CreateAssetMenu(fileName = "Item", menuName = "DB/Item", order = 1)]
public class DataItem : ScriptableObject
{
    public ObjectInfo[] itemProperty = new ObjectInfo[81];
    public ObjectInfo[] stageProperty = new ObjectInfo[27];
    public ObjectInfo[] chapProperty = new ObjectInfo[9];
}

[System.Serializable]
public class ObjectInfo
{
    public string strName = "new item";
    public string strScenario = "new Scenario";
    public bool isBuy = false;
    public bool isLock = true;
    public bool isDiaLock = false;
    public int priceCoin = 100;
    public int priceDia = 100;
    public Sprite imageItem;
}