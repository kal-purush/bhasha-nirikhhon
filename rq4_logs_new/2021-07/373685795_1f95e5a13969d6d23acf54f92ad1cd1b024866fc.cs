using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class PlatformRedirect : MonoBehaviour
{
    public int fpsMenuSceneIndex = 1;
    public int vrMenuSceneIndex = 2;

    void Awake()
    {
        bool gotHeadset = HMDExists.XRPresent();

        if (gotHeadset)
        {
            SceneManager.LoadScene(vrMenuSceneIndex);
        }
        else
        {
            SceneManager.LoadScene(fpsMenuSceneIndex);
        }
    }
}