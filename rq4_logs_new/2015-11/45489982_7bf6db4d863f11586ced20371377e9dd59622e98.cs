using UnityEngine;
using System.Collections;
using System.Collections.Generic;

using System.Linq;

public class AccessoryManager : MonoBehaviour
{
    [System.Serializable]
    public struct NameAndObject
    {
        public string name;
        public GameObject obj;
    }

    [SerializeField]
    private NameAndObject[] HeadAccessories;
    [SerializeField]
    private NameAndObject[] BodyAccessories;
    [SerializeField]
    private NameAndObject[] FootAccessories;

    private Dictionary<string, GameObject> DictHeadAccessories;
    private Dictionary<string, GameObject> DictBodyAccessories;
    private Dictionary<string, GameObject> DictFootAccessories;

    private static AccessoryManager _instance;
    public static AccessoryManager Instance
    {
        get
        {
            if(_instance == null)
            {
                _instance = ((GameObject)Instantiate(Resources.Load("AccessoryManager"), Vector3.zero, Quaternion.identity)).GetComponent<AccessoryManager>();
            }

            return _instance;
        }
    }

    public void Awake()
    {
        DictHeadAccessories = new Dictionary<string, GameObject>();
        foreach(var no in HeadAccessories)
        {
            DictHeadAccessories.Add(no.name, no.obj);
        }
        DictBodyAccessories = new Dictionary<string, GameObject>();
        foreach(var no in BodyAccessories)
        {
            DictBodyAccessories.Add(no.name, no.obj);
        }
        DictFootAccessories = new Dictionary<string, GameObject>();
        foreach(var no in FootAccessories)
        {
            DictFootAccessories.Add(no.name, no.obj);
        }
    }

    public KeyValuePair<string, GameObject>[] GetHeadAccessories()
    {
        return DictHeadAccessories.ToArray();
    }
    public KeyValuePair<string, GameObject>[] GetBodyAccessories()
    {
        return DictBodyAccessories.ToArray();
    }
    public KeyValuePair<string, GameObject>[] GetFootAccessories()
    {
        return DictFootAccessories.ToArray();
    }

    public GameObject GetHeadAccessory(string name)
    {
        return DictHeadAccessories[name];
    }
    public GameObject GetBodyAccessory(string name)
    {
        return DictBodyAccessories[name];
    }
    public GameObject GetFootAccessory(string name)
    {
        return DictFootAccessories[name];
    }
}