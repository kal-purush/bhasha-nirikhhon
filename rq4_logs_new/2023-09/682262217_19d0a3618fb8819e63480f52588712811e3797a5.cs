using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using Unity.VisualScripting;
using UnityEditor.Experimental.GraphView;
using UnityEditor.PackageManager.UI;
using UnityEditor.PackageManager;
using UnityEngine;
using static Unity.Collections.AllocatorManager;
using static UnityEngine.Rendering.VirtualTexturing.Debugging;
using static UnityEngine.UIElements.UxmlAttributeDescription;
using UnityEngine.SceneManagement;
using TMPro;

public class Timer : MonoBehaviour
{
    public TextMeshProUGUI timerText;

    public float timeLimit = 30f;
    private float currentTime;

    void Start()
    {
        currentTime = timeLimit;
    }

    void Update()
    {
        if (currentTime > 0)
        {
            currentTime -= Time.deltaTime;
            timerText.text = currentTime.ToString("F1");
        }
        else
        {
            Debug.Log("Timer has reached zero");
            SceneManager.LoadScene(SceneManager.GetActiveScene().name);
        }
    }
}
