using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossHealth : EnemyBehaviour
{
    public override void Start()
    {
        
    }

    public override void Update()
    {
        if (life <= 0)
        {
            GameManager.Instance.AddScore(scoreGive);
            gameObject.SetActive(false);
            Drop();
            waveManager = GameObject.Find("WaveManager").transform;
            WaveManager.enemiesLeft = WaveManager.enemiesLeft - 1;
            GameManager.Instance.healthBar.SetActive(false);
        }
    }
}