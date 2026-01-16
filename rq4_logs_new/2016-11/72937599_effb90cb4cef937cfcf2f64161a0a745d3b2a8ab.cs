using UnityEngine;
using System.Collections;

public class Movement : MonoBehaviour {
    public GameObject player;
    public float min;
    public float max;
    //private Vector3 camera;
	void Start () {
	
	}
	
	void Update () {
        if (player.transform.position.x > min && player.transform.position.x < max)
        {
            //camera = new Vector3(player.transform.position.x, transform.position.y, transform.position.z);
            //transform.position = Vector3.Lerp(transform.position, camera);
            transform.position = new Vector3(player.transform.position.x, 0, -10);
        }
    }
}