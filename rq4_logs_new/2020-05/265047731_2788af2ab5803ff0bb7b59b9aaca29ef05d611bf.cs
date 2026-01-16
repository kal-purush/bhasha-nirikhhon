using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SFXPlayer : MonoBehaviour
{
    [Header("Player SFX")]
    [SerializeField] AudioClip[] footstepsClips = default;
    [SerializeField] AudioClip[] jumpClips = default;

    [Header("Enemy SFX")]
    [SerializeField] AudioClip[] shooterClips = default;

    [Header("Interactables SFX")]
    [SerializeField] AudioClip[] gemClips = default;
    [SerializeField] AudioClip[] blockHits = default;
    [SerializeField] AudioClip blockBreak = default;

    [Header("Level SFX")]
    [SerializeField] AudioClip winLevelClip = default;
    [SerializeField] AudioClip lifeLostClip = default;

    [Header("Game Session SFX")]
    [SerializeField] AudioClip loseGameClip = default;

    [Header("SFX Mixer")]
    [Range(0f,1f)] [SerializeField] float playerVolume = 0.5f;
    [Range(0f,1f)] [SerializeField] float enemyVolume = 0.3f;
    [Range(0f,1f)] [SerializeField] float interactablesVolume = 0.6f;
    [Range(0f,1f)] [SerializeField] float levelVolume = 0.7f;
    [Range(0f,1f)] [SerializeField] float gameSessionVolume = 0.6f;

    private AudioClip RandomClip(AudioClip[] clips)
    {
        AudioClip clip = clips[UnityEngine.Random.Range(0,clips.Length)];
        return clip;
    }

    public AudioClip GetFootstepsClip()
    {
        return RandomClip(footstepsClips);
    }

    public AudioClip GetJumpClip()
    {
        return RandomClip(jumpClips);
    }

    public AudioClip GetShooterClip()
    {
        return RandomClip(shooterClips);
    }

    public AudioClip GetGemClip()
    {
        return RandomClip(gemClips);
    }

    public AudioClip GetBlockHitClip()
    {
        return RandomClip(blockHits);
    }

    public AudioClip GetBlockBreakClip()
    {
        return blockBreak;
    }

    public AudioClip GetWinLevelClip()
    {
        return winLevelClip;
    }

    public AudioClip GetLifeLostClip()
    {
        return lifeLostClip;
    }

    public AudioClip GetLoseGameClip()
    {
        return loseGameClip;
    }

    public float GetPlayerVolume()
    {
        return playerVolume;
    }

    public float GetEnemyVolume()
    {
        return enemyVolume;
    }

    public float GetInteractablesVolume()
    {
        return interactablesVolume;
    }

    public float GetLevelVolume()
    {
        return levelVolume;
    }

    public float GetGameSessionVolume()
    {
        return gameSessionVolume;
    }

}