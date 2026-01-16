using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class AsyncLoadingScene : MonoBehaviour 
{
    //传递跳转的场景
    public string SceneName;

    //显示加载进度的滑动条
    public Slider slider;

    //异步操作对象
    private AsyncOperation async;

	//-------------------------------------------------

	void Start () 
    {
        StartCoroutine(LoadScene());
	}

    //-------------------------------------------------

    private IEnumerator LoadScene() 
    {
        async = SceneManager.LoadSceneAsync(SceneName);

        //不允许场景加载太快而秒跳场景
        async.allowSceneActivation = false;

        while(!async.isDone)
        {
            yield return async;
        }
    }

    //-------------------------------------------------

    void Update() 
    {
        slider.value += 0.015f;

        if (slider.value >= 1f)
        {
            async.allowSceneActivation = true;
        }
    }

    //-------------------------------------------------
}