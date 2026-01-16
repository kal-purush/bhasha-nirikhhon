using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.PerformanceData;
using UnityEngine;

public class EnemyBehaviour : MonoBehaviour
{
    private Rigidbody2D self;
    [SerializeField] private float moveSpeed;
    [SerializeField] private Rigidbody2D bullet;
    [SerializeField] private float bulletSpeed;
    [SerializeField] private float shootingRate;
    public bool resetShoot;
    private void Start()
    {
        self = GetComponent<Rigidbody2D>();
        self.AddForce(Vector2.down * moveSpeed);
        resetShoot = true;
    }

    private void Update()
    {
        if(resetShoot == true)
            Shooting();
    }

    void Shooting()
    {
        resetShoot = false;
        Debug.Log("wsh");
        Rigidbody2D shotBullet = Instantiate(bullet, transform.position, transform.rotation);
        shotBullet.AddForce(Vector2.down * bulletSpeed);
        Invoke(("ResetShoot"), shootingRate);
    }

    private void ResetShoot()
    {
        resetShoot = true;
    }
}