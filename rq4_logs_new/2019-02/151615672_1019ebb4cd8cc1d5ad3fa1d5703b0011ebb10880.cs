using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObjectPoolerScript : MonoBehaviour {
    
	// Use this for initialization

    [System.Serializable]
    public class Pool
    {
        public string tag;
        public GameObject prefab;
        public int size;
    }

    #region PsuedoSingleton
    public static ObjectPoolerScript Instance;
    private void Awake()
    {
        Instance = this;
    }
    #endregion

    public List<Pool> pools;
    public Dictionary<string, Queue<GameObject>> poolDictionary;
    void Start ()
    {
        poolDictionary = new Dictionary<string, Queue<GameObject>>();

        foreach (Pool pool in pools) //1.)for each pool
        {
            Queue<GameObject> objectPool = new Queue<GameObject>();//2.) make a queue for each pool
            for (int i = 0; i < pool.size; i++)
            {
               GameObject obj = Instantiate(pool.prefab); //3.) populate the pool with objects
                obj.SetActive(false);
                objectPool.Enqueue(obj);//add them to the back of the queue
            }
            poolDictionary.Add(pool.tag, objectPool); // adds pool to dictionary with it's related queue
        }

	}


    public GameObject SpawnFromPool(string tag, Vector3 position, Quaternion rotation)
    {

        if(!poolDictionary.ContainsKey(tag))
        {
            Debug.LogWarning("Pool with tag: " + tag + "doesn't exist!");
            return null;
        }
        GameObject objectToSpawn = poolDictionary[tag].Dequeue();//pulls out first element in queue;
        objectToSpawn.SetActive(true);
        objectToSpawn.transform.position = position;
        objectToSpawn.transform.rotation = rotation;

        IPooledObject pooledObj = objectToSpawn.GetComponent<IPooledObject>();
        if(pooledObj != null)
        {
            pooledObj.OnObjectSpawn();
        }


        poolDictionary[tag].Enqueue(objectToSpawn); //adds the spawned object back to the queue so that it can be reused.

        return objectToSpawn;//allows the same return functionality that Instantiate has;
    }

	
}