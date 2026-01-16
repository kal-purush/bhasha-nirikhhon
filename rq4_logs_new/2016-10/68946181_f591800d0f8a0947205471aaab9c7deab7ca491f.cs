using UnityEngine;
using UnityEngine.Events;
using System.Collections;

public class PlayerDeathManager : MonoBehaviour {

	private UnityAction deathListener; //this is to test player death
	private SpriteRenderer sprite_renderer;


	void Awake ()
	{
		deathListener = new UnityAction (ResetPlayerLocation);
	}
	
	void OnEnable ()
	{
		EventManager.StartListening ("PlayerDead", deathListener);
	}

	void OnDisable ()
	{
		EventManager.StopListening ("PlayerDead", deathListener);
	}


	void ResetPlayerLocation()
	{
		Debug.Log("PlayerDead has been triggered");
	}
}