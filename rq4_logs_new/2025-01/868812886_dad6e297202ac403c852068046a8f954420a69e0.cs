using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI; // For handling UI components
using DG.Tweening;

public class CacheSystem : MonoBehaviour
{
    [SerializeField] private Transform Position1, Position2, Position3;
    [SerializeField] private EnemySpawnerWithIndividualRange enemySpawner;

    [SerializeField] private GunSystem gunSystem;

    [SerializeField] private Transform Player;

    [SerializeField] private AudioSource audioSource;
    [SerializeField] private AudioClip audioClip;

    [SerializeField] private Image BlackScreen; // UI Image for fading effect
    [SerializeField] private float fadeDuration = 1.0f; // Duration of fade-in and fade-out animation

    public void ResetLevel(int level, Action callback = null)
    {
        // Start fade-in animation
        FadeIn(() =>
        {
            // Perform reset logic after fade-in is complete
            switch (level)
            {
                case 1:
                    enemySpawner.InitiateEnemies();
                    Player.position = Position1.position;
                    break;
                case 2:
                    enemySpawner.SpawnEnemies(level - 1);
                    enemySpawner.SpawnEnemies(level);
                    Player.position = Position2.position;
                    break;
                case 3:
                    enemySpawner.SpawnEnemies(level - 1);
                    Player.position = Position3.position;
                    break;
                default:
                    enemySpawner.InitiateEnemies();
                    Player.position = Position1.position;
                    break;
            }

            gunSystem.ResetGun();
            callback?.Invoke();
            PlaySound();

            // Start fade-out animation after resetting level
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

    private void PlaySound()
    {
        if (audioSource != null && audioClip != null)
        {
            audioSource.PlayOneShot(audioClip);
        }
    }
}