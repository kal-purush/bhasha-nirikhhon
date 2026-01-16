using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TriangleBehaviour : MonoBehaviour
{
    [SerializeField] private Transform waveManager;
    [SerializeField] private Rigidbody2D self;
    [SerializeField] private float moveSpeed;
    [SerializeField] private float bulletSpeed;
    [SerializeField] private float shootingRate;
    [SerializeField] private Rigidbody2D bullet;
    private bool resetShoot = true;
    private Vector2 triangleToPlayer;
    [SerializeField] private Transform player;


    private void Start()
    {
        waveManager = GameObject.Find("WaveManager").transform;
        self = GetComponent<Rigidbody2D>();
        player = GameObject.Find("Player").transform;
        self.AddForce(Vector2.down * moveSpeed);
    }

    private void Update()
    {
        if (resetShoot)
        {
            Shooting();
        }
        LookAtPlayer();
    }
    void Shooting()
    {
        resetShoot = false;
        Debug.Log("wsh");
        Rigidbody2D shotBullet = Instantiate(bullet, transform.position, transform.rotation);
        shotBullet.AddForce(triangleToPlayer * bulletSpeed);
        Invoke(("ResetShoot"), shootingRate);
    }

    private void ResetShoot()
    {
        resetShoot = true;
    }
    
    private void LookAtPlayer()
    {
        triangleToPlayer = new Vector2(player.position.x - transform.position.x, player.position.y - transform.position.y ).normalized;
        float angle = Mathf.Atan2(triangleToPlayer.y, triangleToPlayer.x) * Mathf.Rad2Deg;
        self.rotation = angle + 90f;
    }
}