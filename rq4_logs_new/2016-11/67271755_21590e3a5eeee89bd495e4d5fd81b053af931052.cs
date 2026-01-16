using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using System.Collections;

public class MenuScript : MonoBehaviour
{
	public void speechPress()
	{
		SceneManager.LoadScene ("placeholder");

	}

	public void settingsPress()
	{
		SceneManager.LoadScene ("settings");
	}

	public void analyzePress()
	{
		SceneManager.LoadScene ("analyze");
	}

	public void difficultyPress()
	{
		SceneManager.LoadScene ("difficulty");
	}

	public void newUploadPress()
	{
		SceneManager.LoadScene ("upload");
	}
}