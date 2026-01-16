using UnityEngine;
using System.Collections;

public class Enemy : MonoBehaviour {

	Vector3 origPos;
    MainPlayer player;
    float curSpeed; //speed the enemy will move
    float playerDistance; //distance player currently is

    void Awake () {
        //maybe change ranges based on level
        curSpeed = Random.Range(1.0f, 5.0f);
    }
	// Use this for initialization
	void Start () {
        origPos = transform.position; //where the enemy spawns, not ready yet
        tag = "Enemy";
        player = GameObject.Find("Player").GetComponent<MainPlayer>();
	}

	void Move () {
		//always moves towards player, will implement AI eventually
		transform.rotation = Quaternion.LookRotation(
            player.transform.position - transform.position);
        GetComponent<Rigidbody>().MovePosition(transform.position+transform.forward*
            (curSpeed * Time.deltaTime));
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void FixedUpdate () {
		//always check where the player is
        playerDistance = Vector3.Distance(
            player.transform.position, transform.position);
        //don't keep going towards player if close enough
		if (playerDistance > 2f) Move();
	}
}