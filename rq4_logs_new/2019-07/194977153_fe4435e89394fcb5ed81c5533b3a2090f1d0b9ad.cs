using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class titleManager : MonoBehaviour
{
    public Text highScoreText;
    HighScoreManager highScoreManager;
    // Start is called before the first frame update
    void Start()
    {
        highScoreManager = GetComponent<HighScoreManager>();
        highScoreText.text = "Highscore: " + highScoreManager.getHighScore().ToString();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

}