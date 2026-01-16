using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
public class Win : MonoBehaviour
{
    public TextMeshProUGUI time;
    public TextMeshProUGUI score;
    private float scoreCount = 10000;
    // Start is called before the first frame update
    void Start()
    {
        time.text = PlayerPrefs.GetString("Time");

        switch (PlayerPrefs.GetInt("health"))
        {
            case 1:
                {
                    scoreCount = 10000 - 4000 - PlayerPrefs.GetFloat("count") * 10;
                    break;
                }
            case 2:
                {
                    scoreCount = 10000 - 2000 - PlayerPrefs.GetFloat("count") * 10;
                    break;
                }
            case 3:
                {
                    scoreCount = 10000 - 1000 - PlayerPrefs.GetFloat("count") * 10;
                    break;
                }
        }

        int scoreInt = (int)scoreCount;
        score.text = scoreInt.ToString();
    }
}