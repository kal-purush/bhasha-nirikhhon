using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class loadScene : MonoBehaviour
{
    public string _scene;

    private void Start()
    {
        if (string.IsNullOrEmpty(_scene))
            _scene = GetComponent<universalParameters>().getSceneToLoad();
    }

    public void changeToScene()
    {
        SceneManager.LoadScene(_scene);
    }

    public void changeToSpecificScene(string scene)
    {
        SceneManager.LoadScene(scene);
    }
}