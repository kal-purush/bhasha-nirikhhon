using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Gun : Weapon
{
    #region Variables

    [SerializeField, Header("Gun")]
    public GameObject bulletPrefab;
    public float bulletSpeed;
    public float rateOfFire;
    
    private float timer;
    private float timeSinceFired;
    // Gun sound

    #endregion

    #region Methods

    override public void Fire()
    {
        timer += Time.time - timeSinceFired; 
        if (timer > rateOfFire)
        {
            Projectile bullet = Instantiate(bulletPrefab, this.transform.position, this.transform.localRotation).GetComponent<Projectile>();
            bullet.SetSpeed(bulletSpeed, damage);
            timer = 0;
        }

        timeSinceFired = Time.time;
    }
    #endregion
}