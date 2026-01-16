using UnityEngine;
using System.Collections;

/// <summary>
/// This script gives to enemys bullet speed, damage and impact effect.
/// </summary>
public class EnemyShootScript : MonoBehaviour {

	public float speed;

	public PlayerScript player;

	public GameObject impactEffect;

	public int damageToGive;


    // Use this for initialization
    /// <summary>
    /// Makes enemy shoot at right direction
    /// </summary>
    void Start () {
		player = FindObjectOfType<PlayerScript> ();

		if (player.transform.position.x < transform.position.x)
			speed = -speed;

	}

    // Update is called once per frame
    /// <summary>
    /// Gives bullet a speed.
    /// </summary>
    void Update () {

		GetComponent<Rigidbody2D>().velocity = new Vector2 (speed, GetComponent<Rigidbody2D>().velocity.y);

	}

    /// <summary>
    /// Kills player when enemy bullet hits him.
    /// </summary>
    /// <param name="other"></param>
    void OnTriggerEnter2D(Collider2D other){

		Instantiate (impactEffect, transform.position, transform.rotation);
		Destroy (gameObject);

		if (other.name == "Player") {
			Destroy (other.gameObject);
	}
}
}