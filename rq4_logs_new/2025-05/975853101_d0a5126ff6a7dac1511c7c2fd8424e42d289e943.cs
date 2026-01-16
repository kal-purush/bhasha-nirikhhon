using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class SceneChanger_taka : MonoBehaviour
{
    public void LoadSceneByName(string sceneName)
    {
        SceneManager.LoadScene(sceneName);
    }
    public void LoadGameScee()
    {
        SceneManager.LoadScene("GameScene_hara");
    }

    public void LoadScoreScene()
    {
        SceneManager.LoadScene("ScoreScene_Hara");
    }

    public void LoadTitleScene()
    {
        SceneManager.LoadScene("TitleScore_hara");
    }
}