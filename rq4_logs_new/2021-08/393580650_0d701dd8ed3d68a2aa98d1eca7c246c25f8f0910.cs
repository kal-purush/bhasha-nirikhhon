using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Title : MonoBehaviour
{
    [SerializeField] SceneChangeFade m_sceneManager;
    [SerializeField] float m_fadeTime = 2f;
    [SerializeField] string m_titleScene = "";
    // Start is called before the first frame update
    void Start()
    {
       if( Input.GetButtonDown("Fire10"))
        {
            m_sceneManager.LoadSceneWithFade(m_fadeTime, m_titleScene);
        }
    }

    // Update is called once per frame

}