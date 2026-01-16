using UnityEngine;
using System.Collections;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class MainMenu : MonoBehaviour 
{
	MainMenu menu;
	public RawImage logo;
	public Button start;
	public Button quit;
	public Canvas quitMenu;
	
	// Use this for initialization
	void Start () {
		Time.timeScale = 0f;
		menu = GetComponent<MainMenu>();
		quitMenu = quitMenu.GetComponent<Canvas> ();
		start = start.GetComponent<Button>();
		quit = quit.GetComponent<Button>();
		menu.enabled = true;
		logo.enabled = true;
		quitMenu.enabled = false;
	}
	
	// Update is called once per frame
	void Update () {
		
	}
	
	public void ExitGame(){
		quitMenu.enabled = true;
		start.enabled = false;
		quit.enabled = false;
	}
	
	public void NoQuit(){
		quitMenu.enabled = false;
		start.enabled = true;
		quit.enabled = true;
	}
	public void Quit(){
		Application.Quit();
	}
	
	
	public void StartGame()
	{
		Application.LoadLevel (1);	
	}
	
}