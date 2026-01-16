using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScoreDisplay : MonoBehaviour
{
  
    // The UI text component that displays the score
    public Text scoreText;

    // Method to update the display of the score
    public void SetScore(int score)
    {
        // Check if scoreText is assigned to prevent errors
        if (scoreText != null)
        {
            // Update the text component with the new score
            scoreText.text = "Score: " + score.ToString();
        }
        else
        {
            Debug.LogError("ScoreText is not assigned in the Inspector!");
        }
    }
}

