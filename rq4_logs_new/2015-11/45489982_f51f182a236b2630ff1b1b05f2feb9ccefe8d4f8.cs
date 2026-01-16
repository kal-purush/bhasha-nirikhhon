using UnityEngine;
using System.Collections;

public class Bubble : MonoBehaviour {

	public GameObject[] friends;
	public GameObject fox;
	public GameObject ostrich;
	public AudioClip[] captureSound;

	private AudioSource audioSource;
	private GameController gameController;
	private float scaleIncrement = 0.1f;
	private GameObject friendClone;

	void Start()
	{
		gameController = Camera.main.GetComponent<GameController> ();
		audioSource = GetComponent<AudioSource> ();
	}

	void OnCollisionEnter2D(Collision2D other)
	{
		if(other.gameObject.tag == "Fox" || other.gameObject.tag == "Ostrich")
		{
			gameController.AddScore(1);
			audioSource.clip = captureSound[Random.Range (0,captureSound.Length)];
			audioSource.Play();

			if(other.gameObject.tag == "Fox")
				friendClone = (GameObject) Instantiate(fox, friends[0].transform.position, friends[0].transform.rotation);
			else if(other.gameObject.tag == "Ostrich")
				friendClone = (GameObject) Instantiate(ostrich, friends[0].transform.position, friends[0].transform.rotation);

			friendClone.transform.parent = friends[0].transform;

			Vector2 scale = transform.localScale;
			scale.x += scaleIncrement;
			scale.y += scaleIncrement;
			transform.localScale = scale;

			Destroy(other.gameObject);
		}
	}
}