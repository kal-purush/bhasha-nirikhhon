using UnityEngine;
using UnityEngine.SceneManagement;

public class LevelManager : MonoBehaviour
{
    private static LevelManager instance;
    public static LevelManager Instance { get { return instance; } }
    public string Level1;

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }
    }

    void Start()
    {
        if (GetLevelStatus(Level1) == LevelStatus.Locked)
        {
            SetLevelStatus(Level1, LevelStatus.Unlocked);
        }
    }

    public void MarkLevelComplete(string level)
    {
        //set current level status as Complete
        Scene currentScene = SceneManager.GetActiveScene();
        LevelManager.Instance.SetLevelStatus(currentScene.name, LevelStatus.Completed);

        //set next level status as Unlocked
        int NextSceneIndex = currentScene.buildIndex + 1;
        Scene nextScene = SceneManager.GetSceneAt(NextSceneIndex);
        LevelManager.Instance.SetLevelStatus(currentScene.name, LevelStatus.Unlocked);
    }

    public LevelStatus GetLevelStatus(string level)
    {
        LevelStatus levelStatus = (LevelStatus)PlayerPrefs.GetInt(level, 0);
        return levelStatus;
    }

    public void SetLevelStatus(string level, LevelStatus levelStatus)
    {
        PlayerPrefs.SetInt(level, (int)levelStatus);
        Debug.Log("Setting level : " + level + " Status : " + levelStatus);
    }


}