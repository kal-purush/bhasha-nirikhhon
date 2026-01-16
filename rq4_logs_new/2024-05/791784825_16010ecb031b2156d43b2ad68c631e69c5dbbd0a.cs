using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class DynamicSceneLoader : MonoBehaviour
{
    // This method will be called from the Animation Event
    public void LoadScene(string sceneName)
    {
        Debug.Log("LoadScene called with: " + sceneName); // Debug log to ensure the method is called
        if (!string.IsNullOrEmpty(sceneName))
        {
            Debug.Log("Loading scene: " + sceneName);
            SceneManager.LoadScene(sceneName);
        }
        else
        {
            Debug.LogError("Scene name to load is not set or is empty.");
        }
    }
}