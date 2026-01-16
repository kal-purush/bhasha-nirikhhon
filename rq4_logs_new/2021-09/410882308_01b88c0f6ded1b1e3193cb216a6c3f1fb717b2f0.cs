using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class StarBehaviours : EnemyBehaviour
{
    [SerializeField] private Transform spawnPoint1;
    [SerializeField] private Transform spawnPoint2;
    [SerializeField] private Transform spawnPoint3;
    [SerializeField] private Transform spawnPoint4;
    [SerializeField] private Transform spawnPoint5;
    [SerializeField] private Transform directionPoint1;

    public override void Update()
    {
        base.Update();
    }
    

    public override void Shooting()
    {
        resetShoot = false;
        Debug.Log("je tire");
        Rigidbody2D bulletShot1 = PoolManager.Instance.spawnFromPool(bullet, spawnPoint1);
        Rigidbody2D bulletShot2 = PoolManager.Instance.spawnFromPool(bullet, spawnPoint2);
        Rigidbody2D bulletShot3 = PoolManager.Instance.spawnFromPool(bullet, spawnPoint3);
        Rigidbody2D bulletShot4 = PoolManager.Instance.spawnFromPool(bullet, spawnPoint4);
        Rigidbody2D bulletShot5 = PoolManager.Instance.spawnFromPool(bullet, spawnPoint5);
        bulletShot1.AddForce((spawnPoint1.position - directionPoint1.position).normalized * bulletSpeed);
        bulletShot2.AddForce((spawnPoint2.position - directionPoint1.position).normalized* bulletSpeed);
        bulletShot3.AddForce((spawnPoint3.position - directionPoint1.position).normalized * bulletSpeed);
        bulletShot4.AddForce((spawnPoint4.position - directionPoint1.position).normalized * bulletSpeed);
        bulletShot5.AddForce((spawnPoint5.position - directionPoint1.position).normalized * bulletSpeed);
        Invoke(("ResetShoot"), shootingRate);
    }
}