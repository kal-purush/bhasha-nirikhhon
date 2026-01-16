using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using static UnityEngine.Random;

public class PentaBehaviour : MonoBehaviour
{
    private Rigidbody2D self;
    [SerializeField] private float moveSpeed;
    [SerializeField] private PoolManager.Generate bullet;
    [SerializeField] private float bulletSpeed;
    [SerializeField] private float shootingRate;
    public int life = 5;
    public bool resetShoot;
    private Transform waveManager;
    private Vector3 target;
    private Vector3 pentaToTarget;
    private float timeElapsed;
    private float timeUntilStop = 4f;
    private void Start()
    {
        self = GetComponent<Rigidbody2D>();
        self.AddForce(Vector2.down * moveSpeed);
        resetShoot = true;
        timeElapsed = Time.time;
    }

    private void Update()
    {
        if(resetShoot == true)
            Shooting();
        if (life == 0)
        {
            Debug.Log("mort");
            gameObject.SetActive(false);
            waveManager = GameObject.Find("WaveManager").transform;
            WaveManager.enemiesLeft = WaveManager.enemiesLeft - 1;
        }

        if (Time.time >= timeElapsed + timeUntilStop)
        {
            self.velocity = Vector2.zero; 
        }
    }

    void Shooting()
    {
        resetShoot = false;
        target = new Vector2(Random.Range(-3.5f, 3.5f), Random.Range(-4.5f, 1f));
        pentaToTarget = target - transform.position;
        Rigidbody2D shotBullet = PoolManager.Instance.spawnFromPool(bullet, transform);
        shotBullet.AddForce(pentaToTarget.normalized * bulletSpeed);
        Invoke(("ResetShoot"), shootingRate);
    }

    private void ResetShoot()
    {
        resetShoot = true;
    }
}