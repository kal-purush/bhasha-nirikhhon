using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class ScoreKeeper : MonoBehaviour
{
    public int score = 0;
    [SerializeField]
    //TextMeshProUGUI tmp;
    Text scoreText;
    
    
// Start is called before the first frame update
    void Start()
    {
        score = 0;
        

        
    }

    // Update is called once per frame
    void Update()
    {
        scoreText.text = score.ToString();
    }

    public void scoreAdd()
    {
        score++;
    }
        
}