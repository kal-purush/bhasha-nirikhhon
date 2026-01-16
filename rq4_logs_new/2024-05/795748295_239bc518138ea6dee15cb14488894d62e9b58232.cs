using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class SceneManager : MonoBehaviour
{
    public static SceneManager instance;
    [SerializeField] private GameObject sceneTransition;
    private Animator anim;

    private void Awake()
    {
        anim = sceneTransition.GetComponentInChildren<Animator>();
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Debug.Log("Instance of Scene Manager already exists.");
        }
    }

    public void LoadNewScene(string sceneName)
    {
        StartCoroutine(loadSceneRoutine(sceneName));
    }

    IEnumerator loadSceneRoutine(string sceneName)
    {
        anim.SetTrigger("Start");
        yield return new WaitForSeconds(1f);
        UnityEngine.SceneManagement.SceneManager.LoadScene(sceneName);
        anim.SetTrigger("End");
    }




}