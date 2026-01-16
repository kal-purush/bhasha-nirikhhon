using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class SceneManager : Manager<SceneManager>
{
    private Dictionary<string, int> scenes;

    public override void Init()
    {
        base.Init();

        scenes = new Dictionary<string, int>();

        scenes.Add("GameScene", 0);
        scenes.Add("ScheduleScene", 1);
    }
	
    public bool ChangeScene(string sceneName)
    {
        int num;
        if(scenes.TryGetValue(sceneName, out num))
        {
            UnityEngine.SceneManagement.SceneManager.LoadScene(num);
            return true;
        }
        else
        {
            Debug.LogError("sceneName " + sceneName + " DOESN'T EXIST");
            return false;
        }
    }

    public void ChangeScene(int index)
    {
        UnityEngine.SceneManagement.SceneManager.LoadScene(index);
    }
}