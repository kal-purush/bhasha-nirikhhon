using System.Collections;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class LoadingScreenManager : MonoBehaviour
{
    public static LoadingScreenManager Instance;
    public GameObject m_LoadingScreenObject;
    public Slider ProgressBar;
    //[SerializeField] private float minimumLoadingTime = 1f;

    private void Awake()
    {
        if(Instance != null && Instance != this)
        {
            Destroy(this.gameObject);
        }
        else
        {
            Instance = this;
            DontDestroyOnLoad(this.gameObject);
        }
    }
    public void LoadScene(string sceneName)
    {
        StartCoroutine(LoadSceneAsync(sceneName));
    }
    private IEnumerator LoadSceneAsync(string sceneName)
    {
        m_LoadingScreenObject.SetActive(true);
        ProgressBar.value = 0;

        //float fakeProgress = 0f; 
        //float progressSpeed = 0.5f; 
        //float startTime = Time.time;
        //float minimumLoadingTime = 3f;

        AsyncOperation asyncLoad = SceneManager.LoadSceneAsync(sceneName);
        //asyncLoad.allowSceneActivation = true; // Cho phép scene kích hoạt khi tải xong
        asyncLoad.allowSceneActivation = false;
        while (!asyncLoad.isDone) //|| fakeProgress < 1f
        {
            float realProgress = Mathf.Clamp01(asyncLoad.progress / 0.9f);
            //fakeProgress = Mathf.MoveTowards(fakeProgress, realProgress, progressSpeed * Time.deltaTime); yield return null;
            //ProgressBar.value = fakeProgress;
            ProgressBar.value = realProgress;
            if (asyncLoad.progress >= 0.9f)
            {
                asyncLoad.allowSceneActivation = true;
            }
            yield return null;
        }

        //while (fakeProgress < 1f)
        //{
        //    fakeProgress = Mathf.MoveTowards(fakeProgress, 1f, progressSpeed * Time.deltaTime);
        //    ProgressBar.value = fakeProgress;
        //    yield return null;
        //}

        //float elapsedTime = Time.time - startTime;
        //if (elapsedTime < minimumLoadingTime)
        //{
        //    yield return new WaitForSeconds(minimumLoadingTime - elapsedTime);
        //}

        m_LoadingScreenObject.SetActive(false);
    }

}