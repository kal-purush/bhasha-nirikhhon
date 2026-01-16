using UnityEngine;
using System.Collections;

public class Camera : MonoBehaviour {

    public Transform player1;
    public Transform player2;
    public Transform player3;
    public Transform player4;


	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        float averageX = (player1.position.x + player2.position.x) / 2;
        float averageY = (player1.position.y + player2.position.y) / 2;
        float averageZ = (player1.position.z + player2.position.z) / 2;
        transform.position = new Vector3(averageX, 15, averageZ);
	}
}