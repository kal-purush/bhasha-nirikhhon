using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class UIManager : MonoBehaviour
{
    //Lives
    public Sprite[] lives;
    public Image livesImageDisplay;
    public void UpdateLives(int currentLives)
    {
        Debug.Log("The lives current" + currentLives);
        livesImageDisplay.sprite = lives[currentLives];
    }

    public void UpdateScore()
    {

    }
}