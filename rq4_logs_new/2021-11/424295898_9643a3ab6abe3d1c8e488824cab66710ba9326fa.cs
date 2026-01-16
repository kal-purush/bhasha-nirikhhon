using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class LevelOverController : MonoBehaviour
{
    public GameObject levelCompletedText;

    void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.GetComponent<PlayerController>() != null)
        {
            levelCompletedText.SetActive(true);
            Invoke("LoadNextLevel", 3);
        }
    }

    void LoadNextLevel()
    {
        levelCompletedText.SetActive(false);
        SceneManager.LoadScene(1);
    }
}