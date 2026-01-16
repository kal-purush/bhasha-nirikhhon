using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class Destroyer : MonoBehaviour {


	public static Destroyer instance = null;


	public List<GameObject> padsInScene;


	
	// Use this for initialization
	void Start () {
		if (Destroyer.instance == null) {
			instance = this;
		} else if (instance != this) {
			Destroy(gameObject);
		}
	
	}
	
	// Update is called once per frame
	void Update () {

		if(GameStateManager.instance.gameState == GameState.MAIN_MENU)
		{
	
				while(padsInScene.Count != 0)
				{
					Destroy(padsInScene[0]);
					padsInScene.RemoveAt(0);
				}
			}
		}
	

	void OnTriggerEnter(Collider other)
	{
		if(other.gameObject.tag == ("Lilypad"))
		{
			Destroy(other.transform.parent.gameObject);
			padsInScene.RemoveAt(0);
		}
	}
}