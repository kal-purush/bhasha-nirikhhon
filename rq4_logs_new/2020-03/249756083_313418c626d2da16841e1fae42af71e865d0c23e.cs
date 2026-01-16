using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObjectPooler : MonoBehaviour
{
    
    public GameObject store;
    public Swimmer[] swimmers;

    
    List<int> nextAvailable;

    [SerializeField]
    public List<List<GameObject>> poolTypes;



    // Start is called before the first frame update
    void Start()
    {
        
    }

    public void FillPool()
    {
        nextAvailable = new List<int>();

        poolTypes = new List<List<GameObject>>();

        for(int i=0; i<swimmers.Length;i++)
        {
            nextAvailable.Add(0);

            poolTypes.Add(new List<GameObject>());

            for(int j=0; j<swimmers[i].spawnAmount;j++)
            {
                GameObject g = (GameObject)Instantiate(swimmers[i].prefab);
                g.SetActive(false);
                g.transform.parent = store.transform;

                poolTypes[i].Add(g);
            }
        }
    }

    public void Refresh()
    {
        for(int i=0; i<poolTypes.Count;i++)
        {
            nextAvailable[i] = 0;

            for(int j=0; j<poolTypes[i].Count;j++)
            {
                poolTypes[i][j].SetActive(false);
            }
        }
    }


    public GameObject GetNext(ItemType t)
    {
        int type = (int)t;
        int next = nextAvailable[type];

        GameObject g  = new GameObject();
        
        if(next < poolTypes[type].Count)
        {
            g = poolTypes[type][nextAvailable[type]];

            nextAvailable[type]++;
        }
        else
        {
            Debug.LogError("Out of " + System.Enum.GetName(typeof(ItemType),type));
        }

        return g;
    }



    // Update is called once per frame
    void Update()
    {
        /*
        if(Input.GetMouseButtonDown(0))
        {
            GameObject gTest = GetNext(ItemType.TinCan);

            gTest.SetActive(true);
        }

        if(Input.GetMouseButtonDown(1))
        {
            Refresh();
        }*/
        
    }
}

[System.Serializable]
public class Swimmer
{
    public GameObject prefab;

    public int spawnAmount;
}

public enum ItemType
{
    Cereal,
    Bread,
    TinCan
}