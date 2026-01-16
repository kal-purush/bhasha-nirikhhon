using UnityEngine;
using System.Collections;

public class HighscoreController : MonoBehaviour {

	public void OnBackButtonClick()
    {
        Application.LoadLevel("menu");
    }
}