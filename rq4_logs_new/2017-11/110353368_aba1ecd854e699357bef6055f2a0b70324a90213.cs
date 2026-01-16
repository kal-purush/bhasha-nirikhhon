using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ObjectPooler : MonoBehaviour {

	//Object pooling Class code adapted from 
	//https://www.youtube.com/watch?v=ijSRJ3yrawQ&list=PLiyfvmtjWC_XmdYfXm2i1AQ3lKrEPgc9-&index=6
	public GameObject pooledObject;
	public int poolSize;

	public List<GameObject> objectPool;

	// Use this for initialization
	void Start () {
		objectPool = new List<GameObject>();
		for (int i = 0; i < poolSize; i++)
		{
			GameObject obj = (GameObject)Instantiate(pooledObject);
			obj.SetActive(false);
			objectPool.Add(obj);
		}
	}

	// Update is called once per frame
	void Update () {
	}

	public GameObject getPooledObject()
	{
		foreach (GameObject pooledObj in objectPool)
			if (!pooledObj.activeInHierarchy) return pooledObj;               

		//create and add a new object to the pool if there aren't enough to use
		GameObject obj = (GameObject)Instantiate(pooledObject);
		obj.SetActive(false);
		objectPool.Add(obj);
		return obj;
	}


}