using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WorldManager : MonoBehaviour
{
    private void Awake() {
        DontDestroyOnLoad(this);

        // If there is already a world manager, destroy this one
        if (FindObjectsOfType<WorldManager>().Length > 1) {
            Destroy(gameObject);
        }
    }

    private void Start() {
        // Subscribe to the on eval complete event
        GameManager.instance.onEvalCompleted += OnEvalCompleted;
    }

    private void OnEvalCompleted() {
        // When this method is called, determain what the next scene should be, and load it
        // 
    }
}