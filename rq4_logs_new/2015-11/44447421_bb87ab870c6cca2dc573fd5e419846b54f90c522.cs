using UnityEngine;
using System.Collections;

public class EnemyScript : MonoBehaviour {

	public float speed;
	public int type;

	GameControl controller;

	// Use this for initialization
	void Start () {
		controller = GameObject.Find("GameController").GetComponent<GameControl>(); 
	}
	
	// Update is called once per frame
	void Update () {
	
	}

	void OnCollisionEnter2D(Collision2D col)
	{
		if(col.gameObject.tag == "bullet")
		{
			int bulletType = col.gameObject.GetComponent<BulletScript>().Type();
			Destroy(col.gameObject);
			if((type == 0 && bulletType == 2) || (type == 1 && bulletType == 2))
			{
				controller.incrementScore(5);
				Destroy(gameObject);
			}
			else if(type == 4 && bulletType == 3)
			{
				controller.incrementScore(5);
				Destroy(gameObject);
			}
		}
	}
}