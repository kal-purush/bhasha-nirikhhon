using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;
using UnityEngine.SceneManagement;

public class SceneLoader : MonoBehaviour
{
    [SerializeField] Animator transition;
    [SerializeField] float transitionTime = 1f;
    [SerializeField] string loadScene;
    SpawnManager spawnManager;

    // Update is called once per frame
    void Awake()
    {
        spawnManager = FindObjectOfType<SpawnManager>();
    }
    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.tag == "Player")
        {
            if (loadScene != null)
                LoadNextScene(loadScene); // Add in the scene name to the parameter.
        }
    }


    // Load the scene with the paramter as its name
    void LoadNextScene(string sceneName)
    {
        StartCoroutine(LoadAnimation(sceneName));
    }

    // Play the animation
    IEnumerator LoadAnimation(string sceneName)
    {
        // Play animation
        transition.SetTrigger("SceneStart");

        // Wait for animation to finish
        yield return new WaitForSeconds(transitionTime);

        // Load scene
        spawnManager.SetPrevScene();
        
        SceneManager.LoadScene(sceneName);
    }
}