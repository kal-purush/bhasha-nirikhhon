using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class SceneController : MonoBehaviour
{
    public enum SceneType
    {
        Rainforest,
        Island,
        Space,
        Mountains,
        JapaniseGarden
    }

    private void Start()
    {

    }

    public void LoadScene(SceneType scene)
    {
        LoadSceneByIndex((int)scene);
    }

    public void LoadSceneByIndex(int index)
    {
        SceneManager.LoadScene(index);
    }
}