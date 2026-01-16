using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class LevelSelectManager : MonoBehaviour
{
    public TextMeshProUGUI level1BtnText;
    public TextMeshProUGUI level2BtnText;
    public TextMeshProUGUI level3BtnText;
    // Start is called before the first frame update
    void Start()
    {
        // Check which levels have been completed, and assign those buttons' text as play again
        if (PlayerPrefs.GetInt("level1Complete", 0) == 1)
        {
            level1BtnText.text = "Play Again";
        }
        if (PlayerPrefs.GetInt("level2Complete", 0) == 1)
        {
            level2BtnText.text = "Play Again";
        }
        if (PlayerPrefs.GetInt("level3Complete", 0) == 1)
        {
            level3BtnText.text = "Play Again";
        }
    }
}