using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Enemy : MonoBehaviour
{
    private Transform player;
    private float rotationalSpeed; 
    private float enemySpeed;
    public GameObject laserPrefab;
    private float cooldownTimer;
    private float fireDelay;
    public static bool enemyDestroy;
    private float x, y, z;
    // Update is called once per frame
    void Start()
    {
        SetDestroy();
        SetTimer();
        SetRotationalSpeed();
        SetEnemySpeed();
        SetFireDelay();
        StartPosition();

    }
    void Update()
    {
        FaceToPlayer();
        Movement();
        Fire();

    }

    private void SetFireDelay()
    {
        fireDelay = 3f;
    }

    private void SetEnemySpeed()
    {
        enemySpeed = 0.5f;
    }
    private void SetRotationalSpeed()
    {
        rotationalSpeed = 180f;
    }

    private void SetTimer()
    {
        cooldownTimer = 0;
    }

    private void SetDestroy()
    {
        enemyDestroy = false;
    }

    public static bool IsDestroy()
    {
        return enemyDestroy;
    }

    private void Fire()
    {
        cooldownTimer -= Time.deltaTime;
        if (cooldownTimer < 0)
        {
            //shoot
            Vector3 startPoint = new Vector3(transform.position.x, transform.position.y, transform.position.z);
            Instantiate(laserPrefab, startPoint, transform.rotation);
            cooldownTimer = fireDelay;

        }
    }

    private void StartPosition()
    {
        x = Random.Range(-9, 9);
        y = 5.45f;
        z = 0;
        transform.position = new Vector3(x, y, z);
    }

    private void Movement()
    {
        Vector3 velocity = new Vector3(0, enemySpeed * Time.deltaTime, 0);
        Vector3 pos = transform.position;
        pos += transform.rotation * velocity;
        transform.position = pos;
    }

    private void FaceToPlayer()
    {
        if (player == null)
        {
            GameObject go = GameObject.FindWithTag("Player");
            if (go != null)
            {
                player = go.transform;
            }
        }
        //at this point, we eihter have found the player or he/she doesn't exist.
        if (player == null)
        {
            return;//try again next frame!
        }

        Vector3 dir = player.position - transform.position;
        dir.Normalize();
        float zAngle = Mathf.Atan2(dir.y, dir.x) * Mathf.Rad2Deg - 90;
        Quaternion desiredRot = Quaternion.Euler(0, 0, zAngle);
        transform.rotation = Quaternion.RotateTowards(transform.rotation, desiredRot, rotationalSpeed * Time.deltaTime);
    }
    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "Player" || collision.gameObject.tag == "laser1")
        {
            enemyDestroy = true;
            SpaceShooterAbstraction.score += 50;
            Destroy(this.gameObject);
        }
    }


}