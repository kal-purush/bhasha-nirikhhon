using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class RhinoController : MonoBehaviour {

	static int lifes = 50;
	public float speed = 1;
	public GameObject stop;
	public bool isRushing;

	public float spawnbWait;
	public float spawnbMostWait;
	public float spawnbLeastWait;
	public int startbWait;
	public bool bstop;

	void Update()
	{
		transform.Translate (Vector3.forward * speed * Time.deltaTime);

		spawnbWait = Random.Range (spawnbLeastWait, spawnbMostWait);

		if (lifes <= 0) 
		{
			Destroy (gameObject);
		}
	}

	void OnTriggerEnter2D (Collider2D other)
	{
		if (other.gameObject.name == stop.name) 
		{
			speed = 0;
			isRushing = false;
			StartCoroutine (waitSpawner());
		}

		if (other.gameObject.tag == "Bullet") 
		{
			Destroy (other.gameObject);
			lifes = lifes - 10;
			StartCoroutine ("flashmeplox");
		}
	}

	public IEnumerator waitSpawner()
	{
		yield return new WaitForSeconds (startbWait);

		while (!bstop)
		{
			GetComponent<Animator>().Play("Attack");
			
			yield return new WaitForSeconds (spawnbWait);
		}
	}

	IEnumerator flashmeplox()
	{
		for (int i = 0; i < 0.2f; i++)
		{
			GetComponent<SpriteRenderer> ().material.color = Color.black;
			yield return new WaitForSeconds (0.05f);
			GetComponent<SpriteRenderer> ().material.color = Color.white;
			yield return new WaitForSeconds (0.05f);
		}
	}
	
	IEnumerator rhinoRush()
	{
		if (IsRushing == true)
		{
			yield return new WaitForSeconds (startbWait);

			while (!bstop)
			{
				GetComponent<Animator>().Play("Rush");
			
				yield return new WaitForSeconds (spawnbWait);
			}
		}
	}
}