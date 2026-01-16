using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using UnityEngine;

public class Boss2Behaviours : EnemyBehaviour
{
    [SerializeField] private PoolManager.Generate redBullet;
    [SerializeField] private PoolManager.Generate blueBullet;
    [SerializeField] private PoolManager.Generate pentaRedBullet;
    [SerializeField] private PoolManager.Generate pentaBlueBullet;
    private int pattern;
    private PoolManager.Generate bulletToShoot;
    private bool resetPattern = true;
    private float timeElapsed;
    [SerializeField] private GameObject healthBar;
    private float patternTime;
    private int numberOfShots;
    private Vector3 target;
    private Vector2 bossToTarget;

    public override void Start()
    {
        base.Start();
        timeElapsed = Time.time;
        pattern = 1;
        Instantiate(healthBar);
        healthBar.SetActive(true);
        BossHealthBar.Instance.boss = gameObject;
    }

    public override void Update()
    {
        if (Time.time >= timeElapsed + timeUntilStop)
        {
            self.velocity = Vector2.zero;
        }

        if (life <= 0)
        {
            GameManager.Instance.AddScore(scoreGive);
            gameObject.SetActive(false);
            Drop();
            waveManager = GameObject.Find("WaveManager").transform;
            WaveManager.enemiesLeft = WaveManager.enemiesLeft - 1;
            healthBar.SetActive(false);
        }

        if (resetPattern)
        {
            pattern = 0;
            patternTime = 1f;
            resetPattern = false;
        }

        if (patternTime > 0f)
        {
            patternTime -= Time.deltaTime;
        }

        if (patternTime < 0f)
        {
            pattern = Random.Range(1,2);
            patternTime = 0f;
        }

        switch (pattern)
        {
            case 1 :
                resetPattern = false;

                if (resetShoot)
                {
                    Shooting();
                    numberOfShots++;
                }

                if (numberOfShots >= 20)
                {
                    resetPattern = true;
                    resetShoot = false;
                    numberOfShots = 0;
                }
                break;
            
            case 2 :
                break;
            
            case 3 :
                break;
        }
        
    }

    public override void Shooting()
    {
        resetShoot = false;
        bulletSpeed = 75f;
        shootingRate = 2f;
        target = new Vector2(Random.Range(-3.5f, 3.5f), Random.Range(-4.5f, 1f));
        bossToTarget = transform.position - target;
        

    }
}