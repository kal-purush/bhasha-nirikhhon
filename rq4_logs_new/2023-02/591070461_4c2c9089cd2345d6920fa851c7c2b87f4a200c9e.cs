using UnityEngine;
using UnityEngine.SceneManagement;

public enum LevelStatus
{
    locked, unlocked, completed
}
public class LevelManager : MonoBehaviour
{
    private static LevelManager instance = null;
    public static LevelManager Instance { get { return instance; } }

    private void Awake()
    {
        if (instance != null)
        {
            Destroy(instance);
        }
        else
        {
            instance = this;
            DontDestroyOnLoad(instance);
        }
    }

    private void Start()
    {
        if (GetLevelStatus(1) == LevelStatus.locked)
            SetLevelStatus(1, LevelStatus.unlocked);
    }

    public void SetLevelComplete()
    {
        int currentScene = SceneManager.GetActiveScene().buildIndex;
        SetLevelStatus(currentScene, LevelStatus.completed);
        Debug.Log("Level " + currentScene + " has been completed");
        int nextScene = currentScene + 1;
        if (GetLevelStatus(nextScene) == LevelStatus.locked)
            SetLevelStatus(nextScene, LevelStatus.unlocked);
    }

    public void SetLevelStatus(int level, LevelStatus levelStatus)
    {
        PlayerPrefs.SetInt(level.ToString(), (int)levelStatus);
        Debug.Log(level.ToString() + " has been set to " + levelStatus);
    }

    public LevelStatus GetLevelStatus(int level)
    {
        LevelStatus levelStatus = (LevelStatus)PlayerPrefs.GetInt(level.ToString(), 0);
        Debug.Log("Level status of level " + level + " is : " + levelStatus);
        return levelStatus;
    }

    public void LoadLevel(int level)
    {
        Debug.Log("Trying to load level: " + level);
        int index = level;
        switch (GetLevelStatus(index))
        {
            case LevelStatus.locked:
                Debug.Log("The level is locked!");
                break;
            case LevelStatus.unlocked:
                SceneManager.LoadScene(index);
                break;
            case LevelStatus.completed:
                SceneManager.LoadScene(index);
                break;
        }
    }
    public void LoadNextLevel()
    {
        int index = SceneManager.GetActiveScene().buildIndex + 1;
        LoadLevel(index);
    }

}