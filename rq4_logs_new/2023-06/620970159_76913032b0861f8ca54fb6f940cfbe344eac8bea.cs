using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class DisplayCutscene : MonoBehaviour
{
    public void View()
    {
        SceneManager.LoadScene("Intro_cutscene");
    }
}