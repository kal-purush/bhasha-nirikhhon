using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyRangedAttack : MonoBehaviour
{

    public GameObject projectile;
    public Transform target;
    private float projectileSpeed;
    public float timeBetweenShots = 5f;
    private float shotCounter;
    public float distanceMax = 2f;

    void Start()
    {
        shotCounter = timeBetweenShots;
        target = GameObject.FindWithTag("PlayerHitbox").transform;
    }

    void Update()
    {
        shotCounter -= Time.deltaTime;
        if (shotCounter <= 0 && CheckInRange())
        {
            shotCounter = timeBetweenShots;
            Shoot();
        }
    }

    void Shoot()
    {
        float rot = Mathf.Atan2(target.position.y - transform.position.y, target.position.x - transform.position.x) * Mathf.Rad2Deg;
        Vector2 direction = target.position - transform.position;
        GameObject newProjectile = Instantiate(projectile, transform.position, Quaternion.Euler(0, 0, rot - 90f));
        Rigidbody2D rb = newProjectile.GetComponent<Rigidbody2D>();

        projectileSpeed = newProjectile.GetComponent<IProjectile>().Speed;
        rb.velocity = direction.normalized * projectileSpeed;
    }

    bool CheckInRange()
    {
        float distance = Vector2.Distance(transform.position, target.position);
        if (distance < distanceMax)
        {
            return true;
        }

        return false;
    }



}