using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class WinState : MonoBehaviour
{
    public FishingMinigame fishingMinigame;
    public PlayerController playerController;
    public GameObject activateUI;

    void Start()
    {
        activateUI.SetActive(false);
        playerController.isFishing = false;
    }

    void Update()
    {
        // Find a way to find the amount of fish in the scene without making it manual because fuck me
        if (fishingMinigame.fishCount >= 4) {
            activateUI.SetActive(true);
            playerController.isFishing = true;

            if (Input.GetKey(KeyCode.Space)) SceneManager.LoadScene(0);
        }
    }
}