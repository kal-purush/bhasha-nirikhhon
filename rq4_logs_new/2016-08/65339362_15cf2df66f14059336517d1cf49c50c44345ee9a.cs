using UnityEngine;
using System.Collections;

public class CameraController : MonoBehaviour {

    public Vector3 rotation = new Vector3(45, 0, 0);
    public int height = 10;
    public int distanceOffset = 10;

    public GameObject[] players;
	// Use this for initialization
	void Start () {
        transform.position = new Vector3(0, height, 0);
	}

    // Update is called once per frame
    void Update()
    {
        gameObject.transform.rotation = Quaternion.Euler(rotation);

        float x = 0;
        float z = 0;

        for (int i = 0; i < players.Length; i++)
        {
            x += players[i].gameObject.transform.position.x;
            z += players[i].gameObject.transform.position.z;
        }

        gameObject.transform.position = new Vector3(x / players.Length, transform.position.y, z / players.Length - distanceOffset);
    }
}