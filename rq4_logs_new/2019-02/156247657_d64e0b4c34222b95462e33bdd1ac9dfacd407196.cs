using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class AudioManager : MonoBehaviour
{
    static AudioManager instance = null;

    private void Awake()
    {
        if(instance != null)
        {
            Object.Destroy(transform.gameObject);
        }
        else
        {
            instance = this;
            DontDestroyOnLoad(transform.gameObject);
        }
    }

    private void Update()
    {
        if( SceneManager.GetActiveScene().buildIndex != 0  &&
            SceneManager.GetActiveScene().buildIndex != 1 &&
            SceneManager.GetActiveScene().buildIndex != 8 )
        {
            Object.Destroy(transform.gameObject);
        }
    }
}