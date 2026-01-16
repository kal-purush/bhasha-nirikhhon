using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public enum enemyOptions { bat, frog };

[System.Serializable]
public struct enemyEssence
{
    public enemyOptions enemy;
    public AudioClip audioClip;
}

[System.Serializable]
public enum fxOptions { itemPickup, slime, fireball, bonk };
//public enum fxOptions { fireBall, fireballImpact, squish, slime, bat, itemPickup, itemDrop };


[System.Serializable]
public struct audioOptions{
    public fxOptions fx;
    public AudioClip audioClip;
    [Range(0.0f, 1.2f)]
    public float clipVolume;
}

public class AudioManager : MonoBehaviour
{
    public static AudioManager audioManager;

    [Header("Don't repeat enemy options")]
    [SerializeField] enemyEssence[] enemyEssenceOptions = new enemyEssence[] { };

    private void Awake()
    {
        if(audioManager == null)
        {
            audioManager = this;
        } else
        {
            Destroy(gameObject);
        }
    }

    public AudioClip GetEnemyEssenceClip(enemyOptions enemy)
    {
        foreach (var essence in enemyEssenceOptions)
        {
            if(enemy == essence.enemy)
            {
                return enemyEssenceOptions[(int)essence.enemy].audioClip;
            }
        }
        return null;
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.A))
        {
            FXplayer.fxplayer.PlayFX(fxOptions.itemPickup);
        }

        if (Input.GetKeyDown(KeyCode.Q))
        {
            FXplayer.fxplayer.PlayFX(fxOptions.slime);
        }
    }
}