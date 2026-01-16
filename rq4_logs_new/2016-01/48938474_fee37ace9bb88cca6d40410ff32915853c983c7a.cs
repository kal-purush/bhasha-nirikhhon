using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class CameraManager : MonoBehaviour {

    public List<GameObject> players;
    public Vector3 cameraOffset;

	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        Vector3 meanPos = players[0].transform.position;

        for (int i = 1; i < players.Count; i++)
        {
            meanPos = meanPos + players[i].transform.position;
        }
        meanPos.x /= players.Count;
        meanPos.y /= players.Count;
        meanPos.z /= players.Count;

        gameObject.transform.position = meanPos + cameraOffset;


    }
}