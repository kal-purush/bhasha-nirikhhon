using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class BreuLevelController : MonoBehaviour
{
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    #region Load Levels
    public void loadLevel2()
    {
        SceneManager.LoadScene("BreuScene02", LoadSceneMode.Single);
    }
    public void loadLevel3()
    {
        SceneManager.LoadScene("BreuScene03", LoadSceneMode.Single);
    }
    public void loadLevel4()
    {
        SceneManager.LoadScene("BreuScene04", LoadSceneMode.Single);
    }
    public void loadLevel5()
    {
        SceneManager.LoadScene("BreuScene05", LoadSceneMode.Single);
    }

    #endregion
}