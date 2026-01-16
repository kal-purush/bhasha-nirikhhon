using UnityEngine;
using System.Collections;


public class StoryScript : MonoBehaviour {

	enum GAME_STATE {WALK, AVOID, JUDGEMENT, GAME_OVER};

	public GameObject enemy;

	public GameObject player;

	public GameObject Arcade1;

	public GameObject Arcade2;

	public GameObject Surprise_mark;

	public GameObject Circle;

	public GameObject Cross;

	GAME_STATE gameState = GAME_STATE.WALK;

	bool btnAvoid = false;

	//tmp_time
	float timer_avoid = 0;
	float timer_game = 0;
	float timer_judgement = 0;
	float next_time = 0;
	float avoid_interval = 0;
	//judgement_interval is 1 sec
	float time_to_avoid = 0;

	int x = 0;


	GameObject[] Arcade = new GameObject[5];

	// Use this for initialization
	void Start () {
		Circle.SetActive(false);
		Surprise_mark.SetActive(false);
		Cross.SetActive(false);

		enemy.transform.position = player.transform.position + player.transform.forward * 5;

		Arcade[0] = GameObject.Instantiate (Arcade1);
		Arcade[1] = GameObject.Instantiate (Arcade1);
		Arcade[2] = GameObject.Instantiate (Arcade1);
		Arcade[3] = GameObject.Instantiate (Arcade1);
		Arcade[4] = GameObject.Instantiate (Arcade1);

		Arcade[0].transform.position = player.transform.position;
		Arcade[1].transform.position = player.transform.position + player.transform.forward * 20;
		Arcade[2].transform.position = player.transform.position + player.transform.forward * 40;
		Arcade[3].transform.position = player.transform.position + player.transform.forward * 60;
		Arcade[4].transform.position = player.transform.position + player.transform.forward * 80;

		setAvoidState ();
	}

	void setAvoidState(){
		if (timer_game < 15.0f) {
			avoid_interval = 1.5f + 0.5f * timer_game / 15.0f ;
			time_to_avoid = Random.Range (2.0f, 3.0f);
		} else if (timer_game < 30.0f) {
			avoid_interval = Random.Range (1.0f, 1.5f);
			time_to_avoid = Random.Range (1.25f, 2.0f);
		} else if (timer_game < 45.0f) {
			avoid_interval = Random.Range (0.5f - 0.2f * (timer_game - 30.0f) / 15.0f, 1.0f - 0.5f * (timer_game - 30.0f) / 15.0f);
			time_to_avoid = Random.Range (0.5f, 1.25f);
		}

		next_time = timer_game + time_to_avoid;
	}

	void OnGUI(){

		if (GUI.Button (new Rect (100, 100, 100, 100), "avoid")) {
			if(gameState == GAME_STATE.AVOID){
				btnAvoid = true;
			}else{
				gameState = GAME_STATE.GAME_OVER;
			}
		}

		string strToShow;

		if (gameState == GAME_STATE.GAME_OVER) {
			strToShow = "GAME_OVER";
		} else {
			strToShow = "timer_game: " + timer_game + "\n";
			strToShow += "timer_avoid: " + timer_avoid + "\n";
			strToShow += "timer_judgement: " + timer_judgement + "\n";
			strToShow += "next_time: " + next_time + "\n";
			strToShow += "avoid_interval: " + avoid_interval + "\n";
			strToShow += "time_to_avoid: " + time_to_avoid + "\n";
		}
		GUI.TextArea (new Rect (0, 0, 200, 100), strToShow);
	}

	// Update is called once per frame
	void Update () {
		
		Rigidbody player_rb = player.GetComponent<Rigidbody> ();
		Rigidbody enemy_rb = enemy.GetComponent<Rigidbody> ();

		if (gameState == GAME_STATE.WALK) {
			//cacnel the surprise mark
			Circle.SetActive(false);
			Surprise_mark.SetActive(false);
			Cross.SetActive(false);

			//count game time
			timer_game += Time.deltaTime;

			//walk
			player_rb.velocity = player.transform.forward * 2;
			enemy_rb.velocity = enemy.transform.forward * 2;

			//if timer_game reach next_time, enter avoid mode
			if(timer_game > next_time){
				gameState = GAME_STATE.AVOID;
			}
		} else if(gameState == GAME_STATE.AVOID){
			//show surprise mark
			Circle.SetActive(false);
			Surprise_mark.SetActive(true);
			Cross.SetActive(false);

			//count game time
			timer_game += Time.deltaTime;

			//count timer_avoid
			timer_avoid += Time.deltaTime;

			//still walk
			player_rb.velocity = player.transform.forward * 2;
			enemy_rb.velocity = enemy.transform.forward * 2;

			//if timer_avoid reach avoid_interval, enter judgement mode
			if(timer_avoid > avoid_interval){
				gameState = GAME_STATE.JUDGEMENT;
			}
		}else if(gameState == GAME_STATE.JUDGEMENT){

			if(btnAvoid == false){
				gameState = GAME_STATE.GAME_OVER;
				Cross.SetActive(true);
				Circle.SetActive(false);
				Surprise_mark.SetActive(false);
			} else {
				Circle.SetActive(true);
				Surprise_mark.SetActive(false);
				Cross.SetActive(false);
			}

			//count timer_judgement
			timer_judgement += Time.deltaTime;

			//stop
			player_rb.velocity = player.transform.forward * 0;
			enemy_rb.velocity = enemy.transform.forward * 0;



			//if timer_judgement reach 1sec, enter walk mode 
			if(timer_judgement > 1.0f){
				gameState = GAME_STATE.WALK;
				setAvoidState();
				timer_judgement = 0;
				timer_avoid = 0;
				btnAvoid = false;
			}
		}else if(gameState == GAME_STATE.GAME_OVER){
			Cross.SetActive(true);
			Circle.SetActive(false);
			Surprise_mark.SetActive(false);

			//stop
			player_rb.velocity = player.transform.forward * 0;
			enemy_rb.velocity = enemy.transform.forward * 0;
		}
	}
}