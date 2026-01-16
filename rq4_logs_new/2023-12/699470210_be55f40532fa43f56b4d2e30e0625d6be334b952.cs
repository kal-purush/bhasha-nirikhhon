using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI; 

public class LevelManager : MonoBehaviour
{
    public CurrentLevel level;
    public LevelTracker tracker;
    /*
    public Button level1BT;
    public Button level2BT;
    public Button level3BT;
    public Button level4BT;
    public Button level5BT;
    public Button level6BT;
    public Button level7BT;
    public Button level8BT;
    public Button level9BT;
    public Button level10BT;
    public Button level11BT;
    public Button level12BT;
    */

    public void Start()
    {
        tracker.completedLevels.Add(1, false);
        tracker.completedLevels.Add(2, false);
        tracker.completedLevels.Add(3, false);
        tracker.completedLevels.Add(4, false);
        tracker.completedLevels.Add(5, false);
        tracker.completedLevels.Add(6, false);
        tracker.completedLevels.Add(7, false);
        tracker.completedLevels.Add(8, false);
        tracker.completedLevels.Add(9, false);
        tracker.completedLevels.Add(10, false);
        tracker.completedLevels.Add(11, false);
        tracker.completedLevels.Add(12, false);
    }

    public void EnterLevel1()
    {
        level.currentLevel = 1;
        SceneManager.LoadScene("FinalLevel1", LoadSceneMode.Single);
    }
    public void EnterLevel2()
    {
        level.currentLevel = 2;
        SceneManager.LoadScene("FinalLevel2", LoadSceneMode.Single);
    }
    public void EnterLevel3()
    {
        level.currentLevel = 3;
        SceneManager.LoadScene("FinalLevel3", LoadSceneMode.Single);
    }
    public void EnterLevel4()
    {
        level.currentLevel = 4;
        SceneManager.LoadScene("FinalLevel4", LoadSceneMode.Single);
    }
    public void EnterLevel5()
    {
        level.currentLevel = 5;
    }
    public void EnterLevel6()
    {
        level.currentLevel = 6;
    }
    public void EnterLevel7()
    {
        level.currentLevel = 7;
    }
    public void EnterLevel8()
    {
        level.currentLevel = 8;
    }
    public void EnterLevel9()
    {
        level.currentLevel = 9;
    }
    public void EnterLevel10()
    {
        level.currentLevel = 10;
    }
    public void EnterLevel11()
    {
        level.currentLevel = 11;
    }
    public void EnterLevel12()
    {
        level.currentLevel = 12;
    }
}