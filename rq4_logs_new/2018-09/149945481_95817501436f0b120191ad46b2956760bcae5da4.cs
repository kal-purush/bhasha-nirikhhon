using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameController : MonoBehaviour {

	[SerializeField]
	public DayController daylight;
	[SerializeField]
	public Text daytext;
	[SerializeField]
	public Text nighttext;
	[SerializeField]
	public Text wintext;
	[SerializeField]
	public int time = 30;
	[SerializeField]
	public GameObject button;

	private float daySecondsLeft;
	private float nightSecondsLeft;
	private bool gameRunning = true;

	private GameObject spawnedButton = null;

	void Start() {
		daySecondsLeft = time + 0.499999f;
		nightSecondsLeft = time + 0.499999f;
		wintext.enabled = false;
		UpdateDayText(Mathf.RoundToInt(daySecondsLeft).ToString());
		UpdateNightText(Mathf.RoundToInt(nightSecondsLeft).ToString());
		RandomButton();
	}

	void RandomButton() {
		var minwidth = Camera.main.ScreenToWorldPoint(new Vector3(0f, 0f, 0f)).x;
		var maxwidth = Camera.main.ScreenToWorldPoint(new Vector3(Screen.width, 0f, 0f)).x;
		var minheight = Camera.main.ScreenToWorldPoint(new Vector3(0f, 0f, 0f)).y;
		var maxheight = Camera.main.ScreenToWorldPoint(new Vector3(0f, Screen.height, 0f)).y;
		spawnedButton = Instantiate(button, new Vector3(Random.Range(minwidth, maxwidth), Random.Range(minheight, maxheight)), Quaternion.identity);
		spawnedButton.GetComponent<LightToggle>().daylight = daylight;
	}
	
	// Update is called once per frame
	void Update () {
		int updatedSecondsLeft;
		if (daylight.isDay()) {
			daySecondsLeft -= Time.deltaTime;
			updatedSecondsLeft = Mathf.RoundToInt(daySecondsLeft);
			UpdateDayText(updatedSecondsLeft.ToString());
			SetDayTheme();
		} else {
			nightSecondsLeft -= Time.deltaTime;
			updatedSecondsLeft = Mathf.RoundToInt(nightSecondsLeft);
			UpdateNightText(updatedSecondsLeft.ToString());
			SetNightTheme();
		}

		if (gameRunning && updatedSecondsLeft == 0) {
			wintext.text = daylight.isDay() ? "Le jour gagne!" : "La chauve-souris gagne!";
			wintext.enabled = true;
			if (spawnedButton != null) {
				Destroy(spawnedButton);
			}
			gameRunning = false;
		}

		if (gameRunning && spawnedButton == null) {
			RandomButton();
		}
	}

	void UpdateDayText(string text) {
		if (gameRunning) {
			daytext.text = "Jour: " + text + "s";
		}
	}

	void UpdateNightText(string text) {
		if (gameRunning) {
			nighttext.text = "Nuit: " + text + "s";
		}
	}

	void SetNightTheme() {
		nighttext.color 
		= wintext.color
		= daytext.color = new Color(205 / 255f, 205 / 255f, 205 / 255f);
	}

	void SetDayTheme() {
		nighttext.color 
		= wintext.color
		= daytext.color = new Color(50 / 255f, 50 / 255f, 50 / 255f);
	}
}