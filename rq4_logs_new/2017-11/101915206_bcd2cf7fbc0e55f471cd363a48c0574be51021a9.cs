using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;


public class loadingScript : MonoBehaviour {
    public GameObject Testing;
    public GameObject loadingScreenControl;
    public Slider loadingScreenSlider;

    AsyncOperation loadingAsync;


    public void loadScene(int sceneIndex)
    {
        StartCoroutine(loadingScene(sceneIndex));
    }

    IEnumerator loadingScene(int sceneIndex)
    {
        loadingScreenControl.SetActive(true);
        loadingAsync = SceneManager.LoadSceneAsync(sceneIndex);
        loadingAsync.allowSceneActivation = false;

        while(loadingAsync.isDone == false)
        {
            loadingScreenSlider.value = loadingAsync.progress;

            if(loadingAsync.progress == 0.9f)
            {
                loadingAsync.allowSceneActivation = true;
                loadingScreenSlider.value = 1f;
            }

            yield return null;
        }
    }


	
}