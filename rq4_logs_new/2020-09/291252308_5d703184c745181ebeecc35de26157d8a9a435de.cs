using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class ReStartButtonHandler : MonoBehaviour
{
    Button restartButton;

    void Start()
    {
        restartButton = GetComponent<Button>();
        DataController dataController = FindObjectOfType<DataController>();
        BackgroundAudio backgroundAudio = FindObjectOfType<BackgroundAudio>();
        restartButton.onClick.AddListener(delegate {
            Destroy(dataController);
            Destroy(backgroundAudio);
            SceneManager.LoadScene("PersistentScene");
        });
     
    }
}