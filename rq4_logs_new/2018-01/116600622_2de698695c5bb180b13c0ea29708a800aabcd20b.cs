using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class GunController : MonoBehaviour
{
    public Transform muzzle;
    public GunType equippedGun;

    public void Fire(string team)
    {
        GameObject bullet = (GameObject)GameObject.Instantiate(equippedGun.bulletPrefab, muzzle.position, muzzle.rotation);
        bullet.GetComponent<Rigidbody2D>().velocity = muzzle.right * equippedGun.shotSpeed;
        bullet.GetComponent<Missile>().damage = equippedGun.damage;
        bullet.GetComponent<Missile>().team = team;
        NetworkServer.Spawn(bullet);
    }
}