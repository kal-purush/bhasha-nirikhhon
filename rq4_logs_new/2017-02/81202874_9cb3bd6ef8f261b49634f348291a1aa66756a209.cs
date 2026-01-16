using UnityEngine;
using System.Collections;

public class Bullet : MonoBehaviour {

	public	float	Duration=2f;
	public	float	Speed=4f;

	void	Awake() {
		gameObject.SetActive (false);		//Don't show yet
	}

	public	void	Fire(Vector3 tSpeed) {
		gameObject.SetActive (true);
		GetComponent<Rigidbody2D> ().velocity = tSpeed*Speed;
		Destroy (gameObject, Duration);		//Self Destruct
	}
	void OnTriggerEnter2D(Collider2D vOther) {   //Must use 2D version
		Asteroid tA=vOther.gameObject.GetComponent<Asteroid>();
		if (tA) {
			Destroy (gameObject);
			GM.PlayerShip.Score += tA.Score;		//Right score for that asteroid
			tA.Split ();		//Tell asteroid to split
		}
	}
}