using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerSoundHandler : MonoBehaviour
{
    //Player Component Reference
    [Header("---Component Reference---")]
    [SerializeField] private PlayerCombat playerCombat;
    [SerializeField] private PlayerMovement playerMovement;
    [SerializeField] private AudioSource playerAudio;

    void Start()
    {
        
    }

    void Update()
    {
        HandleAudio();
    }

    private void HandleAudio()
    {
        if (playerCombat.NeutralAttack == true && playerCombat.ComboTally == 1)
        {
            Debug.Log("Slash1");
            playerAudio.clip = Resources.Load<AudioClip>("Audio/Slash1");
            playerAudio.Play();
        }
        else if (playerCombat.NeutralAttack == true && playerCombat.ComboTally == 2)
        {
            playerAudio.clip = Resources.Load<AudioClip>("Audio/Slash2");
            playerAudio.Play();
        }
        else if (playerCombat.NeutralAttack == true && playerCombat.ComboTally == 3)
        {
            playerAudio.clip = Resources.Load<AudioClip>("Audio/Slash3");
            playerAudio.Play();
        }
    }
}