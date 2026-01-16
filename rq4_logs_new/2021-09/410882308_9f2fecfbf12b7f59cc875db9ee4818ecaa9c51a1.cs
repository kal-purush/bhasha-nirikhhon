using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using static UnityEngine.Random;
public class BossBehaviour : MonoBehaviour
{
    [SerializeField] private Rigidbody2D rb;
    [SerializeField] private float moveSpeed;
    [SerializeField] private float bulletSpeed;
    [SerializeField] private PoolManager.Generate redBullet;
    [SerializeField] private PoolManager.Generate blueBullet;
    [SerializeField] float shootingRate;
    private bool patternFinished = true;
    private int pattern;
    private float elapsedTime;
    private float timeUntilStop = 4f;
    private float targetY = -9.5f;
    private float targetX = -4f;
    private bool resetShoot = true;
    private PoolManager.Generate bulletToShoot = PoolManager.Generate.RedBullet;
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
        rb.AddForce(Vector2.down * moveSpeed);
        elapsedTime = Time.deltaTime;
    }

    void Update()
    {
        if (Time.time >= elapsedTime + timeUntilStop)
        {
            rb.velocity = Vector2.zero;
        }
        
        if (patternFinished)
        {
            pattern = 1;

            switch (pattern)
            {
                case 1 :
                    
                    if (resetShoot)
                    {
                        Shooting();
                    }

                    break;
                case 2 :
                    break;
                case 3 :
                    break;
            }
        }
    }

    private void Shooting()
    {
        resetShoot = false;
        
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
    
    private void ResetShoot()
    {
        resetShoot = true;
    }
}