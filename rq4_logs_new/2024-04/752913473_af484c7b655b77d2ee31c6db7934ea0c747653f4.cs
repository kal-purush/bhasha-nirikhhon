using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class PlayerStats : MonoBehaviour
{

    // Start is called before the first frame update
    void Start()
    {
        float attempts = PlayerPrefs.GetInt("attempts", 0);
        int wins = PlayerPrefs.GetInt("wins", 0);
        int highScore = PlayerPrefs.GetInt("highScore", 0);

        transform.GetChild(0).GetComponent<TextMeshProUGUI>().text = attempts.ToString();
        transform.GetChild(1).GetComponent<TextMeshProUGUI>().text = wins.ToString();
        transform.GetChild(2).GetComponent<TextMeshProUGUI>().text = "$" + highScore;
    }
}