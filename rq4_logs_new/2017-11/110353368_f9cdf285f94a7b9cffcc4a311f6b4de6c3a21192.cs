using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.SceneManagement;
using UnityEngine.Rendering;

public class PauseMenu : MonoBehaviour {

	[Header("Required Object Refs.")]
	[Space]

	private GameObject _eventSystem;
	private GameManager _gameManager;

	[Space]
	[Header("UI Container Objects")]
	[Space]

	public GameObject pauseMenuUI;

	public EventList startGameEventList = new EventList();
	public EventList returnToMainMenuEventList = new EventList();

	void Start () {

		_eventSystem = GameObject.Find("EventSystem");
		_gameManager = GameObject.FindGameObjectWithTag("GameManager").GetComponent<GameManager>();
		pauseMenuUI.SetActive(true);
	}

	public void MainMenu() {
		_gameManager.ReturnToMainMenu ();
	}

	public void ReturnToGame() {
		if(pauseMenuUI.activeInHierarchy) {
			pauseMenuUI.SetActive(false);
			Time.timeScale = 1;
		}
	}

	public void RestartGame() {
		SceneManager.LoadScene ("Level");
	}
}