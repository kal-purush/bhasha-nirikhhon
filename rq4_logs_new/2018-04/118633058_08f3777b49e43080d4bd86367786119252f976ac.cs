using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WeaponFire : MonoBehaviour {

    public float damage = 10;
    public float projectileSpeed;
    public float rateOfFire = 0;
    public LayerMask objectsToDamage;

    private Transform firePoint;
    float timeToFire = 0;

    ShootProjectile projectile;

    void Awake()
    {
        firePoint = transform.Find("weaponFP");
        //GameObject weapon = GetComponent<GameObject>();
        //GameObject child = Helper.FindComponentInChildWithTag(weapon, "FiringPoint");
        //Transform firePoint = child.transform;
        //projectile.SetDamage(damage);
        //projectile.SetSpeed(projectileSpeed);
    }

    void Update()
    {
        //Shoot();
        if (rateOfFire == 0)
        {
            if (Input.GetKeyDown(KeyCode.Mouse0))
            {
                //projectile.Shoot(firePoint);
                Shoot();
            }
        }
        else
        {
            if (Input.GetKey(KeyCode.Mouse0) && Time.time > rateOfFire)
            {
                timeToFire = Time.time + 1 / rateOfFire;
                Shoot();
                //projectile.Shoot(firePoint);
            }
        }
    }

    void Shoot()
    {
        Vector2 mousePosition = new Vector2(Camera.main.ScreenToWorldPoint(Input.mousePosition).x, Camera.main.ScreenToWorldPoint(Input.mousePosition).y);
        Vector2 firePointPosition = new Vector2(firePoint.position.x, firePoint.position.y);
        RaycastHit2D hit = Physics2D.Raycast(firePointPosition, mousePosition - firePointPosition, 100, objectsToDamage);
        Debug.DrawLine(firePointPosition, 100*(mousePosition-firePointPosition));
        //Debug.Log("test");
        if(hit.collider != null)
        {
            Debug.DrawLine(firePointPosition, hit.point,Color.red);
        }
        if (hit.collider.name.Equals("Enemy"))
        {
            Debug.Log("enemy hit");
        }
    }

}