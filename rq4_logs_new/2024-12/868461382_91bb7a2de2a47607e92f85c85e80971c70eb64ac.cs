using UnityEngine;

public class AudioManager : MonoBehaviour
{
    [Header("Footstep Settings")]
    public AudioClip[] footstepSounds; // Array of footstep audio clips
    public float minStepInterval; // Minimum time between footsteps
    public float maxStepInterval; // Maximum time between footsteps

    [Header("Attack Sounds")]
    public AudioClip fireAttackSound; // Fire attack audio clip
    public AudioClip iceAttackSound; // Ice attack audio clip
    public AudioClip waterAttackSound; 

    private AudioSource audioSource;
    private bool isWalking = false;
    private float stepTimer = 0f;
    private bool isFireAttackActive = false;
    private bool isIceAttackActive = false;
    private bool isWaterAttackActive = false;

    void Start()
    {
        audioSource = GetComponent<AudioSource>();
    }

    void Update()
    {
        HandleFootsteps();
    }

    void HandleFootsteps()
    {
        if (isWalking)
        {
            stepTimer -= Time.deltaTime;
            if (stepTimer <= 0f)
            {
                PlayRandomFootstep();
                stepTimer = Random.Range(minStepInterval, maxStepInterval);
            }
        }
    }

    void PlayRandomFootstep()
    {
        if (footstepSounds.Length > 0)
        {
            int index = Random.Range(0, footstepSounds.Length);
            AudioClip clip = footstepSounds[index];
            audioSource.PlayOneShot(clip);
        }
    }

    public void StartWalking()
    {
        isWalking = true;
    }

    public void StopWalking()
    {
        isWalking = false;
    }


    public void StopAttack()
    {
        audioSource.Stop();
        audioSource.loop = false;
        isFireAttackActive = false;
        isIceAttackActive = false;
        isWaterAttackActive = false;
    }
    public void PlayAttack(int attackType)
    {
        StopAttack();
        switch (attackType)
        {
            case 1:
                PlayFireAttack();
                break;
            case 2:
                PlayIceAttack();
                break;
            case 3:
                PlayWaterAttack();
                break;
        }
    }
    public void PlayFireAttack()
    {
        if (!isFireAttackActive)
        {
            audioSource.clip = fireAttackSound;
            audioSource.loop = true;
            audioSource.Play();
            isFireAttackActive = true;
        }
    }

    public void PlayIceAttack()
    {
        if (!isIceAttackActive)
        {
            audioSource.clip = iceAttackSound;
            audioSource.loop = true;
            audioSource.Play();
            isIceAttackActive = true;
        }
    }
    
    
    public void PlayWaterAttack()
    {
        if (!isWaterAttackActive)
        {
            audioSource.clip = waterAttackSound;
            audioSource.loop = true;
            audioSource.Play();
            isWaterAttackActive = true;
        }
    }
    
}