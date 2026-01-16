using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ExampleShoot : ZombieShootBase
{
    [SerializeField] private GameObject bullet;
    [SerializeField] private float fireDelay = 2.0f;

    private float timeLeft = 0;

    // Start is called before the first frame update
    void Start()
    {
        base.Start();

        timeLeft = fireDelay;
    }

    // Update is called once per frame
    void Update()
    {
        if (timeLeft <= 0 && !IsMoving())
        {
            timeLeft = fireDelay;
            Shoot();
        }
        else
        {
            timeLeft -= Time.deltaTime;
        }
    }

    public override bool IsShooting()
    {
        return false;
    }

    void Shoot()
    {
        Instantiate(bullet, transform.position, Quaternion.identity);
    }
}