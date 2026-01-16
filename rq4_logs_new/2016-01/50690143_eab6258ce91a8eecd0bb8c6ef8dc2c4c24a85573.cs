using UnityEngine;
using System.Collections;

public class PlatformGenerator : MonoBehaviour {

    public GameObject platformPrefab;
    private GameObject LastPlatform;

	// Use this for initialization
	void Start ()
    {
        Random.seed = (int)System.DateTime.Now.Ticks;
        CreateInitialPlatform();
        for (int i = 0; i < 10; i++)
            CreatePlatform();
        	
	}

    void CreateInitialPlatform()
    {
        var playerPos = GameObject.Find("Player").transform.position;
        GameObject newPlatform = (GameObject)Instantiate(platformPrefab, playerPos + new Vector3(0.0f, -9.0f, 0.0f), new Quaternion());
        newPlatform.transform.parent = this.transform;
        newPlatform.tag = "Ground";
        LastPlatform = newPlatform;
    }

    void CreatePlatform()
    {
        float yPos = Random.value * 8 - 4 - 8;
        float xPos = Random.value * 8 - 4;
        GameObject newPlatform = (GameObject)Instantiate(platformPrefab, LastPlatform.transform.position + new Vector3(7.0f + xPos, -LastPlatform.transform.position.y + yPos, 0.0f), new Quaternion());
        newPlatform.transform.parent = this.transform;
        newPlatform.tag = "Ground";
        LastPlatform = newPlatform;
    }
	
	// Update is called once per frame
	void Update () {
        if (LastPlatform == null || GameObject.FindGameObjectWithTag("Player") == null) return;
        var distanceToLastPlatform = LastPlatform.transform.position.x - GameObject.FindGameObjectWithTag("Player").transform.position.x;

        if (distanceToLastPlatform < 30.0f)
        {
            CreatePlatform();
        }

        var childTransforms = GetComponentInChildren<Transform>();
        foreach(Transform child in childTransforms)
        {
            if(child.position.x + 35.0f < GameObject.FindGameObjectWithTag("Player").transform.position.x)
            {
                Destroy(child.gameObject);
            }
        }

	}
}