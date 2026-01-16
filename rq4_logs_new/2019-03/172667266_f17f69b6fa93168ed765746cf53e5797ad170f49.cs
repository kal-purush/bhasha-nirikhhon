using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DiamondSpawner : MonoBehaviour {

    public GameObject goodDiamond;
    public GameObject badDiamond;
    float randX;
    float randY;
    Vector2 whereToSpawn;
    public float spawnRate = 2f;
    float nextSpawnGood = 0.0f;
    float nextSpawnBad = 0.0f;

    void InstantiategoodDiamond() 
    {
        if (Time.time > nextSpawnGood)
        {
            nextSpawnGood = Time.time + spawnRate;
	    Debug.Log("nextSpawnGood" + nextSpawnGood);
            randX = Random.Range(-8.5f, 8.5f);
	    randY = Random.Range(-4.0f, 4.0f);
            whereToSpawn = new Vector2(randX, randY);
	    Debug.Log("InstantiategoodDiamond");
            Instantiate(goodDiamond, whereToSpawn, Quaternion.identity);
        }
    }
   
    void Update() 
    {
        if(randomizeDiamonds()>1.5f) 
        {
	    InstantiategoodDiamond(); 
	} 
        else 
        {
	    InstantiatebadDiamond();
	}
    }
  


    void InstantiatebadDiamond() 
    {
        if (Time.time > nextSpawnBad)
        {
	    nextSpawnBad = Time.time + spawnRate;
	    Debug.Log("nextSpawnBad" + nextSpawnBad);
            randX = Random.Range(-8.5f, 8.5f);
	    randY = Random.Range(-4.0f, 4.0f);
            whereToSpawn = new Vector2(randX, randY);
	    Debug.Log("InstantiatebadDiamond");
	    Instantiate(badDiamond, whereToSpawn, Quaternion.identity);
        }
    }

    float randomizeDiamonds() 
    {
	return Random.Range(0.0f, 3.0f);
    }


}



