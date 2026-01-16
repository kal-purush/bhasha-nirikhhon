using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI; // For handling UI components
using DG.Tweening;
using System;

public class PuzzleExitController : MonoBehaviour
{
    [SerializeField] private Transform Player;
    [SerializeField] private Transform Destination;
    [SerializeField] private Image BlackScreen; // UI Image for fading effect
    [SerializeField] private float fadeDuration = 1.0f; // Duration of fade-in and fade-out animation
    // Start is called before the first frame update

    private bool hasCalled = false;


    void OnTriggerEnter(Collider other)
    {
        // Check if the colliding object is the player
        if (other.CompareTag("Player") && !hasCalled && AppHelper.HasTalkedFinalNPC)
        {
            hasCalled = true;
            ExitTemple();
        }
    }

    private void ExitTemple()
    {
        FadeIn(() =>
        {
            Player.position = Destination.position;
            GamePhaseManager.instance.gunSystem.ResetGun();
            FadeOut();
        });
    }


    private void FadeIn(Action onComplete = null)
    {
        if (BlackScreen != null)
        {
            BlackScreen.gameObject.SetActive(true); // Ensure BlackScreen is active
            BlackScreen.DOFade(1f, fadeDuration).OnComplete(() => onComplete?.Invoke());
        }
    }

    private void FadeOut()
    {
        if (BlackScreen != null)
        {
            BlackScreen.DOFade(0f, fadeDuration).OnComplete(() =>
            {
                BlackScreen.gameObject.SetActive(false); // Disable BlackScreen after fading out
            });
        }
    }
}