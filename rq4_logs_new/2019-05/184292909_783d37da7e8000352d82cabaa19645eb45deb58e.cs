using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
public class LeaderboardGUIController : MonoBehaviour
{
    [Header("Leaderboard")]
    public TextMeshProUGUI[] names;
    public TextMeshProUGUI[] numbers;

    // [Header("Submit Score System")]

    private void OnEnable(){
        UpdateLeaderboard();
    }

    void UpdateLeaderboard(){
        List<Score> scores = Leaderboard.Scores;
        for (int i = 0; i < names.Length; i++)
        {
            if(i < scores.Count){
                names[i].SetText(scores[i].name);
                numbers[i].SetText(scores[i].value.ToString());
            }
            else{
                names[i].SetText("---");
                numbers[i].SetText("0");
            }
        }
    }
}