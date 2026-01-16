using UnityEngine;
using System.Collections;

public class Manager : MonoBehaviour {

	public GameObject player;
	private GameObject title;

	// Use this for initialization
	void Start () {
		title = GameObject.Find("Title");
	}
	
	// Update is called once per frame
	void Update () {
		if(IsPlaying() == false && Input.GetKeyDown(KeyCode.X)){
			GameStart();
		}
	}

	
	public void GameStart(){
		title.SetActive(false);
		Instantiate(player, player.transform.position, player.transform.rotation);
	}

	public void GameOver(){
		title.SetActive(true);
	}

	public bool IsPlaying(){
		return title.activeSelf == false;
	}



}