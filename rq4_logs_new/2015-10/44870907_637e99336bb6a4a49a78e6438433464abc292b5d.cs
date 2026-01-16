using UnityEngine;
using System.Collections;

public class GameManager : MonoBehaviour {
	[Header("Status")]
	public bool countingDown;
	public float timer = 60f;
	public int score;

	[Header("Balance")]
	public float timePowerupRewardAmt = 5f;
	public static GameManager instance;

	public void OnDestroyBuilding() {
		score += 1;
	}

	void Awake() {
		if( instance == null ) {
			instance = this;
		} else {
			Destroy( this );
		}
	}

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
		timer -= Time.deltaTime;
		if( timer < 0 ) {
			GameOver();
		}
	}

	private void GameOver() {
		Debug.Log( "GAME OVER" );
	}



}