using UnityEngine;
using System.Collections;

public class GameManager : MonoBehaviour {

	public static GameManager Instance;	//Instance is a singleton
	private GameState gameState;

	public enum GameState {
		MainMenu,
		LoadGameScene,
		ChooseCharacterScene,
		GamePlay,
		BattleScene,
		LoseScene,
		NextLevelScene,
		WinScene
	}

	void Awake() {

		if (Instance == null)
			Instance = this;
		else if (Instance != this)
			Destroy (this);

		DontDestroyOnLoad (Instance);
	}

	// Use this for initialization
	void Start () {
		gameState = GameState.MainMenu;
	}
	
	// Update is called once per frame
	void Update () {

		if (gameState == GameState.MainMenu)
			Application.LoadLevel ("MainMenuScene");
		else if (gameState == GameState.LoadGameScene)
			Application.LoadLevel ("LoadGameScene");
		else if (gameState == GameState.ChooseCharacterScene)
			Application.LoadLevel ("ChooseCharacterScene");
		else if (gameState == GameState.GamePlay)
			Application.LoadLevel ("GamePlayScene");
		else if (gameState == GameState.BattleScene)
			Application.LoadLevel ("BattleScene");
		else if (gameState == GameState.LoseScene)
			Application.LoadLevel ("LoseScene");
		else if (gameState == GameState.NextLevelScene)
			Application.LoadLevel ("NextLevelScene");
		else if (gameState == GameState.WinScene)
			Application.LoadLevel ("WinScene");
	
	}

	public void SetGameState(string name)
	{
		gameState = GameState.ChooseCharacterScene;
	}
}