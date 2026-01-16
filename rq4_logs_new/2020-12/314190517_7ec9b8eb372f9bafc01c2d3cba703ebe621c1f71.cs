using System.Collections.Generic;
using UnityEngine;

public class LoadPrefab
{
    private readonly Dictionary<string, GameObject> prefabMap;
    private readonly GameObject[] prefabs;
    private readonly string[] keys;
    private int index;

    // Start is called before the first frame update
    public LoadPrefab()
    {
        this.prefabMap = new Dictionary<string, GameObject>();

        this.prefabs = Resources.LoadAll<GameObject>("360photo/Prefab/");
        foreach(GameObject prefab in this.prefabs)
        {
            this.prefabMap.Add(prefab.name, prefab);
        }

        this.keys = new string[this.prefabMap.Count];
        this.prefabMap.Keys.CopyTo(this.keys, 0);

        this.index = 0;

        this.ChangePrefab(0);
    }

    public void ChangePrefab(int relIndex)
    {
        this.index = (this.index + relIndex + this.keys.Length) % this.keys.Length;

        GameObject emptyObject = GameObject.Find("EmptyObject");
        foreach(Transform child in emptyObject.transform)
        {
            GameObject.Destroy(child.gameObject);
        }

        GameObject prefab = this.prefabMap[this.keys[this.index]];
        GameObject instance = GameObject.Instantiate(prefab, new Vector3(0.0f, 0.0f, 0.0f), Quaternion.identity);
        instance.transform.parent = emptyObject.transform;
    }
}