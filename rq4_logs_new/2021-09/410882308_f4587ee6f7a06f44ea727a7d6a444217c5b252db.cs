using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossBehaviours : EnemyBehaviour
{
    [SerializeField] private PoolManager.Generate redBullet;
    [SerializeField] private PoolManager.Generate blueBullet;
    private int pattern;
    private float elapsedTime;
    private float timeUntilStop = 2f;
    private float targetY = -9.5f;
    private float targetX = -4f;
    private PoolManager.Generate bulletToShoot = PoolManager.Generate.RedBullet;
    private int numberOfShots = 0;
    [SerializeField] private Transform blueSpawnPoint;
    [SerializeField] private Transform redSpawnPoint;
    private bool blueGoToZero = false;
    private bool redGoToZero = false;
    private bool resetPattern = true;
    private float patternTime;
    private float patternChange = 2f;

    public override void Start()
    {
        base.Start();
        elapsedTime = Time.time;
        pattern = Random.Range(1, 4);
    }

    public override void Update()
    {
        if (Time.time >= elapsedTime + timeUntilStop)
        {
            self.velocity = Vector2.zero;
        }
        
        if (life <= 0)
        {
            GameManager.Instance.AddScore(scoreGive);
            Debug.Log("mort");
            gameObject.SetActive(false);
            Drop();
            waveManager = GameObject.Find("WaveManager").transform;
            WaveManager.enemiesLeft = WaveManager.enemiesLeft - 1;
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
            pattern = Random.Range(1, 4);
            Debug.Log(pattern);
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
                    resetPattern = false;
                    
                    if (resetShoot)
                    {
                        Shooting2();
                        numberOfShots++;
                    }

                    if (numberOfShots >= 20)
                    {
                        resetPattern = true;
                        resetShoot = false;
                        numberOfShots = 0;
                        
                    }
                    break;
                
                case 3 :
                    resetPattern = false;
                    if (resetShoot)
                    {
                        Shooting3();
                        numberOfShots++;
                    }

                    if (numberOfShots >= 100)
                    {
                        resetPattern = true;
                        numberOfShots = 0;
                        resetShoot = false;
                        
                    }
                    break;
            }

        
        
    }

    public override void Shooting()
    {
        resetShoot = false;
        bulletSpeed = 200f;
        shootingRate = 0.25f;
        
        for (int i = 0; i < 18; i++)
        {
            
            Vector3 line = new Vector2(targetX + i/2f, targetY);
            Rigidbody2D bulletShot = PoolManager.Instance.spawnFromPool(bulletToShoot, transform);
            bulletShot.AddForce(line.normalized * bulletSpeed);
        }

        if (bulletToShoot == PoolManager.Generate.RedBullet)
        {
            bulletToShoot = PoolManager.Generate.BlueBullet;
        }
        else
        {
            bulletToShoot = PoolManager.Generate.RedBullet;
        }

        Invoke(("ResetShoot"), shootingRate);
    }
    private void Shooting2()
    {
        resetShoot = false;
        bulletSpeed = 200f;
        shootingRate = 0.25f;
        for (int i = 0; i < 18; i++)
        {
            
            Vector3 line = new Vector2(targetX + i/2f, targetY);
            Rigidbody2D bulletShot = PoolManager.Instance.spawnFromPool(bulletToShoot, transform);
            bulletShot.AddForce(line.normalized * bulletSpeed);
            if (bulletToShoot == PoolManager.Generate.RedBullet)
            {
                bulletToShoot = PoolManager.Generate.BlueBullet;
            }
            else
            {
                bulletToShoot = PoolManager.Generate.RedBullet;
            }
        }
        Invoke(("ResetShoot"), shootingRate);
    }

    private void Shooting3()
    {
        resetShoot = false;
        shootingRate = 0.05f;
        bulletSpeed = 300f;
        if (blueSpawnPoint.position.x >= -1f && blueGoToZero == false)
        {
            blueSpawnPoint.position = new Vector2(blueSpawnPoint.position.x - 0.1f, blueSpawnPoint.position.y);
            
        }
        else if (blueSpawnPoint.position.x >= 1f)
        {
            blueGoToZero = false;
        }
        else if (blueSpawnPoint.position.x >= -1f && blueGoToZero)
        {
            blueSpawnPoint.position = new Vector2(blueSpawnPoint.position.x + 0.1f, blueSpawnPoint.position.y);
        }
        else if (blueSpawnPoint.position.x <= -1f)
        {
            blueSpawnPoint.position = new Vector2(blueSpawnPoint.position.x + 0.1f, blueSpawnPoint.position.y);
            blueGoToZero = true;
        }
       

        if (redSpawnPoint.position.x <= 1f && redGoToZero == false)
        {
            redSpawnPoint.position = new Vector2(redSpawnPoint.position.x + 0.1f, redSpawnPoint.position.y);
        }
        
        else if (redSpawnPoint.position.x <= -1f)
        {
            redGoToZero = false;
        }
        
        else if(redSpawnPoint.position.x <= 1f && redGoToZero)
        {
            redSpawnPoint.position = new Vector2(redSpawnPoint.position.x - 0.1f, redSpawnPoint.position.y);
            
        }
        
        else if (redSpawnPoint.position.x >= 1f)
        {
            redSpawnPoint.position = new Vector2(redSpawnPoint.position.x - 0.1f, redSpawnPoint.position.y);
            redGoToZero = true;
        }
        
        Rigidbody2D redBulletShot = PoolManager.Instance.spawnFromPool(redBullet, redSpawnPoint);
        redBulletShot.AddForce(Vector2.down * bulletSpeed);
        Rigidbody2D blueBulletShot = PoolManager.Instance.spawnFromPool(blueBullet, blueSpawnPoint);
        blueBulletShot.AddForce(Vector2.down * bulletSpeed);
        Invoke(("ResetShoot"), shootingRate);
    }
    
}

