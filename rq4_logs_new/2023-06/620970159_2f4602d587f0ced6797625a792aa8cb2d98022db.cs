using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class MusicManager : MonoBehaviour
{
    private static MusicManager instance;

    private void Awake()
    {
        // Check if there is an existing instance of MusicManager
        if (instance != null)
        {
            Destroy(gameObject); // Destroy the duplicate object
            return;
        }

        instance = this;
        DontDestroyOnLoad(gameObject); // Keep the MusicManager object between scenes
    }

    public AudioSource audioSource; // Reference to the AudioSource component for playing music
    public AudioClip menuMusic; // Music clip for the menu
    public AudioClip gameMusic; // Music clip for the game
    public AudioClip battleMusic; // Music clip for battles
    public AudioClip lowHealthMusic; // Music clip for low health
    public AudioClip deathMusic; // Music clip for player death
    public AudioClip bossMusic; // Music clip for boss battles
    public AudioClip creditsMusic; // Music clip for credits

    public float fadeSpeed = 0.5f; // Volume transition speed
    public float battleMusicVolume = 0.5f; // Volume for battle music
    public float lowHealthMusicVolume = 0.8f; // Volume for low health music
    public float lowHealthMusicThreshold = 20f; // Health threshold to activate low health music

    private bool isInBattle = false; // Indicates if the player is in a battle
    private bool isTransitioning = false; // Indicates if a music transition is in progress
    private bool isLowHealth = false; // Indicates if the player has low health
    private bool isDead = false; // Indicates if the player is dead
    private bool isBossBattle = false; // Indicates if it's a boss battle
    private float targetVolume = 1f; // Target volume for transitioning music
    private float initialVolume; // Initial volume before the transition

    private PlayerController playerController; // Reference to the player controller
    private WolfDirection wolfDirection; // Reference to the wolf controller

    private void Start()
    {
        Scene currentScene = SceneManager.GetActiveScene();
        if (IsMenuScene(currentScene))
        {
            PlayMusic(menuMusic); // Play menu music on start
        }
        else if (IsGameScene(currentScene))
        {
            PlayMusic(gameMusic); // Play game music on start
        }

        playerController = FindObjectOfType<PlayerController>(); // Find the player controller
        wolfDirection = FindObjectOfType<WolfDirection>(); // Find the wolf controller
    }

    private void Update()
    {
        if (!isBossBattle && DetectBoss())
        {
            StartBossBattle(); // Start transition to battle music if boss is detected
        }
        else if (isBossBattle && !DetectBoss() && !DetectEnemies())
        {
            EndBossBattle(); // Start transition to credits music
        }
        else if (!isInBattle && DetectEnemies() && (!IsWolfLowHealth() && !IsLowHealth()) && !isBossBattle)
        {
            isInBattle = true;
            StartCoroutine(TransitionToBattleMusic()); // Start transition to battle music if enemies are detected and there is no low health
        }
        else if (!isInBattle && DetectEnemies() && (IsWolfLowHealth() || IsLowHealth()) && !isBossBattle)
        {
            isInBattle = true;
            StartCoroutine(TransitionToLowHealthMusic()); // Start transition to low health music if enemies are detected
        }
        else if (isInBattle && !DetectEnemies() && !isBossBattle)
        {
            isInBattle = false;
            StartCoroutine(TransitionToOriginalMusic()); // Switch back to the original music
        }

        if (!isLowHealth && IsLowHealth() && DetectEnemies() && !isBossBattle)
        {
            isLowHealth = true;
            StartCoroutine(TransitionToLowHealthMusic()); // Start transition to low health music if there is low health and enemies
        }
        else if (isLowHealth && !IsLowHealth() && !IsWolfLowHealth() && !isBossBattle)
        {
            isLowHealth = false;
            if (DetectEnemies())
            {
                isInBattle = true;
            }
            else
            {
                isInBattle = false;
            }
            if (!isInBattle)
            {
                StartCoroutine(TransitionToOriginalMusic()); // Switch back to the original music if there is no low health and no battle
            }
            else
            {
                StartCoroutine(TransitionToBattleMusic()); // Switch back to battle music if there is no low health but there is a battle
            }
        }

        if (!isDead && playerController.health <= 0)
        {
            isDead = true;
            StartCoroutine(TransitionToDeathMusic()); // Start transition to death music if player's health reaches zero
        }
    }

    private IEnumerator TransitionToBattleMusic()
    {
        if (isTransitioning) yield break; // If there is already a transition in progress, exit

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime; // Gradually decrease the volume
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = battleMusic;
        audioSource.volume = battleMusicVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime; // Gradually increase the volume
            yield return null;
        }

        isTransitioning = false;
    }

    private IEnumerator TransitionToLowHealthMusic()
    {
        if (isTransitioning) yield break;

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime;
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = lowHealthMusic;
        audioSource.volume = lowHealthMusicVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime;
            yield return null;
        }

        isTransitioning = false;
    }

    private IEnumerator TransitionToOriginalMusic()
    {
        if (isTransitioning) yield break;

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime;
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = gameMusic;
        audioSource.volume = initialVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime;
            yield return null;
        }

        isTransitioning = false;
    }

    private IEnumerator TransitionToDeathMusic()
    {
        if (isTransitioning) yield break;

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime;
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = deathMusic;
        audioSource.volume = targetVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime;
            yield return null;
        }

        isTransitioning = false;
    }

    private IEnumerator TransitionToBossMusic()
    {
        if (isTransitioning) yield break;

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime;
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = bossMusic;
        audioSource.volume = targetVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime;
            yield return null;
        }

        isTransitioning = false;
    }

    private IEnumerator TransitionToCreditsMusic()
    {
        if (isTransitioning) yield break;

        isTransitioning = true;
        initialVolume = audioSource.volume;

        while (audioSource.volume > 0f)
        {
            audioSource.volume -= fadeSpeed * Time.deltaTime;
            yield return null;
        }

        audioSource.Stop();
        audioSource.clip = creditsMusic;
        audioSource.volume = targetVolume;
        audioSource.Play();

        while (audioSource.volume < targetVolume)
        {
            audioSource.volume += fadeSpeed * Time.deltaTime;
            yield return null;
        }

        isTransitioning = false;
    }

    private bool DetectEnemies()
    {
        GameObject[] enemies = GameObject.FindGameObjectsWithTag("Enemy");
        return enemies.Length > 0; // Returns true if at least one enemy is found
    }
    private bool DetectBoss()
    {
        GameObject[] enemies = GameObject.FindGameObjectsWithTag("Boss");
        return enemies.Length > 0; // Returns true if at least one boss is found
    }

    private bool IsLowHealth()
    {
        return playerController.health < lowHealthMusicThreshold; // Returns true if player health is lower than the low health threshold
    }

    private bool IsWolfLowHealth()
    {
        return wolfDirection.health < lowHealthMusicThreshold; // Returns true if wolf health is lower than the low health threshold
    }

    private void PlayMusic(AudioClip musicClip)
    {
        audioSource.Stop();
        audioSource.clip = musicClip;
        audioSource.volume = targetVolume;
        audioSource.Play();
    }

    private bool IsMenuScene(Scene scene)
    {
        return scene.name == "Initial" || scene.name == "Log_in" || scene.name == "Sign_up"; // Returns true if the scene is a menu scene
    }

    private bool IsGameScene(Scene scene)
    {
        return scene.name == "Game"; // Returns true if the scene is a game scene
    }

    public void StartBossBattle()
    {
        isBossBattle = true;
        StartCoroutine(TransitionToBossMusic()); // Start transition to boss battle music
    }

    public void EndBossBattle()
    {
        isBossBattle = false;
        StartCoroutine(TransitionToCreditsMusic()); // Start transition to credits music
    }
}