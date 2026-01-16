using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{
    public static GameManager instance;
    public InputManager inputManager;
    public GlobalPlayerPosition globalPlayerPosition;
    public SceneManager sceneManager;
    public SpawnManager spawnManager;
    public GameObject sceneTransistion;

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Debug.LogWarning("There is already an instance of GameManager in the scene");
        }

        inputManager = GameObject.Find("InputManager").GetComponent<InputManager>();
        globalPlayerPosition = ScriptableObject.CreateInstance<GlobalPlayerPosition>();
        sceneManager = GameObject.Find("SceneManager").GetComponent<SceneManager>();
        spawnManager = GameObject.Find("SpawnManager").GetComponent<SpawnManager>();
        sceneTransistion = GameObject.Find("SceneTransition");

    }
}