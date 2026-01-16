using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;

/***
 * Main Menu Controller
 * 
 * Basic navigation around the game.  Includes
 * instructions, Quit and level loading.
 */
public class MainMenuController : MonoBehaviour {

	public Texture2D cursorTexture;
	public CursorMode cursorMode = CursorMode.Auto;

	// Use this for initialization
	void Start () {
	
		// TODO: checek for saved games, and enable load if we have some
		Debug.Log(System.Environment.Version);
		Debug.Log(System.Environment.OSVersion);

		Cursor.SetCursor(cursorTexture, Vector2.zero, cursorMode);
	}
	
	// Update is called once per frame
	void Update () {

	}

	// called before start.
	void Awake () {

	}

	private IEnumerable LoadLevel() {
		AsyncOperation async = Application.LoadLevelAsync("outside");
		yield return async;
		Debug.Log ("MM:Loading Level");	
	}

	public void StartGame() {
		// Load the first level!
		Cursor.SetCursor(null, Vector2.zero, cursorMode);

		Application.LoadLevel("Game");
	}
	
	public void QuitGame() {
		Debug.Log ("MM:Quit called");
		Application.Quit();
	}
	
	public void ShowInstructions() {
		Debug.Log ("MM:Instructions Opened");
		SceneManager.LoadScene("instructions");
	}

	public void ShowMainMenu() {
		// Load the first level!
		Cursor.SetCursor(cursorTexture, Vector2.zero, cursorMode);
		Application.LoadLevel("main");

	}

	public void JumpToLevel(string levelName) {
		// Load the first level!
		Application.LoadLevel(levelName);
	}

}