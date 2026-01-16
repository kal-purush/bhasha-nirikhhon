using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class Menu_Controller : MonoBehaviour {

	[Tooltip("sceneToLoadOnPlay is the name of the scene that will be loaded when users click play")]
	public string sceneToLoadOnPlay = "Level";
	[Tooltip("webpageURL defines the URL that will be opened when users click on your branding icon")]
	public string webpageURL = "http://nicky-jones.co.uk";
	[Tooltip("soundButtons define the SoundOn[0] and SoundOff[1] Button objects.")]
	public Button[] soundButtons;
	[Tooltip("audioSource defines the Audio Source component in this scene.")]
	public AudioSource audioSource;
    [Tooltip("IngameMenu is the GameObject that will load the menu when playing in the levels")]
    [SerializeField]
    private GameObject ingameMenu;

	void Awake () {
		if(!PlayerPrefs.HasKey("_Mute")){
			PlayerPrefs.SetInt("_Mute", 0);
		}
	}
	/// <summary>
    /// Opens webpage of a given url by the user
    /// </summary>
	public void OpenWebpage () {
		Application.OpenURL(webpageURL);
	}

	/// <summary>
    /// Changes to the current level that the user has specified
    /// </summary>
	public void PlayGame () {
		UnityEngine.SceneManagement.SceneManager.LoadScene(sceneToLoadOnPlay);
	}
	
    /// <summary>
    /// changes the value for "_Mute" in playerprefs to 1 muting the music
    /// </summary>
	public void Mute () {
		soundButtons[0].interactable = true;
		soundButtons[1].interactable = false;
		PlayerPrefs.SetInt("_Mute", 1);
	}

    /// <summary>
    /// changes the value for "_Mute" in playerprefs to 0 unmuting the music
    /// </summary>
    public void Unmute () {
		soundButtons[0].interactable = false;
		soundButtons[1].interactable = true;
		PlayerPrefs.SetInt("_Mute", 0);
	}

	/// <summary>
    /// Opens the in game menu 
    /// </summary>
    public void inGameMenuOpen()
    {
        if(ingameMenu != null && ingameMenu.activeInHierarchy != true)
        {
            ingameMenu.SetActive(true);
        }
    }

    /// <summary>
    /// Closes the in game menu
    /// </summary>
    public void inGameMenuClose()
    {
        ingameMenu.SetActive(false);
    }

    /// <summary>
    /// Quits the game
    /// </summary>
	public void QuitGame () {
		#if !UNITY_EDITOR
			Application.Quit();
		#endif
		
		#if UNITY_EDITOR
			UnityEditor.EditorApplication.isPlaying = false;
		#endif
	}
}