using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UIElements;

[RequireComponent(typeof(SceneFader))]
public class TitleScreenManager : MonoBehaviour
{
    public GameObject fadeScreen;
    private SceneFader fader;

    // Start is called before the first frame update
    void Start()
    {
        fader = GetComponent<SceneFader>();
        fadeScreen.SetActive(false);
    }

    public void ToScene(string sceneName)
    {
        fadeScreen.SetActive(true);
        StartCoroutine(fader.FadeAndLoadScene(SceneFader.FadeDirection.In, sceneName));
    }
}