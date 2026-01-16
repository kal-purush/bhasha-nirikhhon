using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Turrets : MonoBehaviour {
    #region Bullet Vars
    public float coolDown; //Time before each bullet is fired
    public float attackTimer; //Time before the attack can start, resets after every attack is complete
    float curAtTime;

    public GameObject bulletPref;
    public PatternData[] attacks;
    int curAttack; //Attacks will be run based on order of attack array, then reset one it reaches the last one
    int bulletsFired = 0;
    bool attacking; //Is the enemy attacking now?
    #endregion

    public float damping;
    int curRot; //What rotation are we on now?
    bool rotating;

    void Start ()
    {
        curAtTime = attackTimer;
	}
	
	void Update ()
    {
        #region Firing Bullets
        if (curAtTime <= 0)
        {
            attacking = true;
            curAttack = Random.Range(0, attacks.Length);

            if (bulletsFired <= attacks[curAttack].bulletsInPattern)
            {
                attacking = true;
                if (!IsInvoking("SpawnBullet"))
                {
                    Invoke("SpawnBullet", coolDown);
                    bulletsFired++;
                }
            }
            else
            {
                curAtTime = attackTimer;
                attacking = false;
                bulletsFired = 0;
            }
        }
        else if (curAtTime > 0 && !attacking)
        {
            curAtTime -= Time.deltaTime;
        }
        #endregion

        //Rotating Turret
        if (attacking)
        {
            if (!IsInvoking("RotateTurret"))
            {
                RotateTurret();
            }
            if (transform.rotation == Quaternion.Euler(0, 0, attacks[curAttack].rotations[curRot]))
            {
                rotating = false;
            }

            if (!rotating)
            {
                if (curRot >= attacks[curAttack].rotations.Length - 1)
                {
                    curRot = 0;
                }
                else
                {
                    curRot++;
                }
            }
        }
    }

    public void SpawnBullet()
    {
        bulletPref.GetComponent<BulletScript>().speed = attacks[curAttack].bulletSpeed;
        bulletPref.GetComponent<BulletScript>().destroyTime = attacks[curAttack].destroyTime;
        Instantiate(bulletPref, transform.position, transform.rotation);
    }

    public void RotateTurret()
    {
        rotating = true;
        Quaternion nRot =  Quaternion.Euler(0, 0, attacks[curAttack].rotations[curRot]);
        transform.rotation = Quaternion.LerpUnclamped(transform.rotation, nRot, Time.deltaTime * damping);
    }
}