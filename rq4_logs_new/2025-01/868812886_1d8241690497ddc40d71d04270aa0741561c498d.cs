using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ending : MonoBehaviour
{
    public UnityEngine.UI.Slider slider; // Reference to the Slider component

    void Update()
    {
        if (slider.value >= 99)
        {
            LoadSceneWithIndex1();
        }
    }

    public void LoadSceneWithIndex1()
    {
        UnityEngine.SceneManagement.SceneManager.LoadScene(1);
    }
}