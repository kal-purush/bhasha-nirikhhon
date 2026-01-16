using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

//Basic class to persist an object between scenes, but to only have it visible on the scene it was initialized in
public class Persist : MonoBehaviour
{
    private static Dictionary<Tuple<GameObject, string>, bool> persistSceneTargets = new Dictionary<Tuple<GameObject, string>, bool>();
    private Tuple<GameObject, string> key;

    void Start()
    {
        SceneManager.activeSceneChanged += ChangedActiveScene;

        key = GetDictionaryKey();
        if(persistSceneTargets.ContainsKey(key)) {
            Destroy(gameObject); //Destroy the object if it has already been cloned
        } else {
            DontDestroyOnLoad(this);
            persistSceneTargets[key] = true;
        }
    }

    private Tuple<GameObject, string> GetDictionaryKey() {
        if(key == null) {
            return new Tuple<GameObject, string>(gameObject, SceneManager.GetActiveScene().name);
        } else {
            return key;
        }
    }

    void ChangedActiveScene(Scene c, Scene n) {
        if(this == null)
            return;

        Tuple<GameObject, string> currentKey = GetDictionaryKey();
        if(persistSceneTargets.ContainsKey(currentKey)) {
            Debug.Log(n.name == currentKey.Item2 ? "Enable" : "Disable");
            this.gameObject.SetActive(n.name == currentKey.Item2);
        }
    }
}