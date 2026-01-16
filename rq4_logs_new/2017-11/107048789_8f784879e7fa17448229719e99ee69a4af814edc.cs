using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;
using UnityEngine.UI;

public class TimerController : NetworkBehaviour {
	public Button button;

	int readyPlayers;
	private List<CarController> players = new List<CarController> ();
	private float time;
	private bool countdownOn = false;
	private bool timerOn = false;

	[SyncVar(hook = "UpdateCountdownText")]
	string countdownText = "";

	public static TimerController GetInstance () {
		return GameObject.Find ("Timer").GetComponent<TimerController> ();
	}

	public void OnStartLocal () {
		button.onClick.AddListener (OnReady);
	}

	public void AddPlayer (CarController player) {
		players.Add (player);
	}

	void OnReady () {
		CmdOnPlayerReady ();
	}

	[Command]
	void CmdOnPlayerReady () {
		if (++readyPlayers == players.Count) {
			time = 3f;
			countdownText = "3";
			countdownOn = true;
		}
	}

	void UpdateCountdownText (string newCountdownText) {
		Text text = GameObject.Find ("WinText").GetComponent<Text>();
		text.text = newCountdownText;
	}

	void Update () {
		if (countdownOn) {
			time -= Time.deltaTime;
			if (time <= 0 && time >= -1) {
				countdownText = "Start!";
				foreach (CarController player in players) {
					player.raceActive = true;
				}
			}

			if (time < -1) {
				countdownText = "";
				time = 0;
				countdownOn = false;
				timerOn = true;
			}

			if (time > 0) {
				countdownText = Mathf.Ceil (time).ToString ();
			}
		}

		if (timerOn) {
			time += Time.deltaTime;

			foreach (CarController player in players) {
				player.RpcUpdateTimeText (time);
			}
		}
	}
}