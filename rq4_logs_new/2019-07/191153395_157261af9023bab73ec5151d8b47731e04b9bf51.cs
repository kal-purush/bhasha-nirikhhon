using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class NextSceneButton : MonoBehaviour {

    public Animator animator;

    private void Start()
    {
        Cursor.lockState = CursorLockMode.None;
        SceneTransition.setAnimator(animator);

    }

    public void RetryGame()
    {
        SceneTransition.setScene(LoadPrevScene.getLastLevel());
        SceneTransition.getScene();
        StartCoroutine(SceneTransition.LoadScene());
    }

    public void Mainmenu()
    {
        SceneTransition.setAnimator(animator);
        SceneTransition.setScene("Mainmenu");
        SceneTransition.getScene();
        StartCoroutine(SceneTransition.LoadScene());

    }

    public void ExitGame()
    {
        Application.Quit();
    }
}