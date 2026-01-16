using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

//this tells, this monobehaviour can only be attached to a gameobject that has a component of this particular type attached to it
[RequireComponent(typeof(Button))]
public class LevelLoader : MonoBehaviour
{

    public Button button;
    public string levelName;

    void Awake()
    {
        button = GetComponent<Button>();
        button.onClick.AddListener(OnClick);
    }

    public void OnClick()
    {
        SceneManager.LoadScene(levelName);
    }
}