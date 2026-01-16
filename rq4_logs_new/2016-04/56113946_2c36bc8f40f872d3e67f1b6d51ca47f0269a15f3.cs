using UnityEngine;
using System.Collections;

public class LevelController : MonoBehaviour {

	public void OnEasyButtonCLick()
    {
        Constant.level = 1;
    }

    public void OnMediumButtonClick()
    {
        Constant.level = 2;
    }

    public void OnInsaneButtonClick()
    {
        Constant.level = 3;
    }

    public void OnBackButtonClick()
    {
        Application.LoadLevel("map");
    }

    public void OnQuitToMenu()
    {
        Application.LoadLevel("menu");
    }

    public void OnStartGame()
    {

    }
}