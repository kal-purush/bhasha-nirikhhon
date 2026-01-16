using UnityEngine;
using TMPro;

public class ScoreSceneUIManager_Hara : MonoBehaviour
{
    public TextMeshProUGUI scoreText;

    void Start()
    {
        int score = 0;

        if (ScoreManager.Instance != null)
        {
            score = ScoreManager.Instance.score;
        }
        else
        {
            Debug.LogWarning("ScoreManager.Instance is null, fallback to PlayerPrefs");
            score = PlayerPrefs.GetInt("Score", 0); // 念のため保存された値も見る
        }

        if (scoreText != null)
        {
            scoreText.text = "Score: " + score;
        }
    }
}