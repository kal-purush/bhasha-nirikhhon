using UnityEngine;
using System.Collections;
using UnityEngine.UI;
#if UNITY_EDITOR
using UnityEditor;
#endif

public class PauseManager : MonoBehaviour
{
    RectTransform myRect;
    bool isFullscreen;


    void Start()
    {
        myRect = GetComponent<RectTransform>();
        isFullscreen = false;
    }

    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Tab))
        {
            Pause();
        }
        if (Input.GetKeyDown(KeyCode.Escape))
        {
            Quit();
        }
    }

    public void Pause()
    {
        isFullscreen = isFullscreen == false ? true : false;
        if (isFullscreen)
        {
            myRect.sizeDelta = new Vector2(800, 450);
        }
        else
            myRect.sizeDelta = new Vector2(150, 150);
        Time.timeScale = Time.timeScale == 0 ? 1 : 0;
    }

    public void Quit()
    {
#if UNITY_EDITOR
        EditorApplication.isPlaying = false;
#else
        Application.Quit();
#endif
    }
}