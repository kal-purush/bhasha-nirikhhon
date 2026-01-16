using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;
using UnityEngine.Rendering;

public class GameManager : MonoBehaviour {

    [Header("Managed Object Ref.")]
    [Space]

    private GameObject _eventSystem;

    [Space]
    [Header("Scene Indexer")]
    [Space]

    [SerializeField]
    private int currentSceneIndex;

    [SerializeField]
    private int nextSceneIndex;

    void Awake() {

        currentSceneIndex = SceneManager.GetActiveScene().buildIndex;
        nextSceneIndex = (SceneManager.GetActiveScene().buildIndex + 1);
    }

    /*
     * This section covers all aspects of instantiating, restarting and progressing scenes
     */

    void Start () {

        _eventSystem = GameObject.Find("EventSystem");
    }

    public void ReturnToMainMenu() {
        SceneManager.LoadSceneAsync(0);
    }

    public void ProgressToNextScene() {
        SceneManager.LoadSceneAsync(nextSceneIndex);
    }

    public void ResetScene() {

        SceneManager.LoadSceneAsync(currentSceneIndex);
    }

    public void ExitGame() {
        Application.Quit();
    }
}