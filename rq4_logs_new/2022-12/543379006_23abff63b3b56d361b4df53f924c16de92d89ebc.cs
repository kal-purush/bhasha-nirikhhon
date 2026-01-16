using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Gun : MonoBehaviour
{
    [SerializeField] private GameObject[] bulletPrefabs;
    [SerializeField] private Transform spawnPoint;
    [SerializeField] private float gunSpeed;
    [SerializeField] private AudioSource gunSoundPlayer;
    [SerializeField] private AudioClip gunSound;


    GameObject tempBullet;

    int randomBulletIndex;
    public void shoot()
    
    {
        randomBulletIndex = Random.Range(0, bulletPrefabs.Length);
        
        //pick a random bullet and instatiate

        tempBullet = Instantiate(bulletPrefabs[randomBulletIndex], spawnPoint.position, spawnPoint.rotation);

        //shoot the bullet in the forward direction of the spawnpoint 

        tempBullet.GetComponent<Rigidbody>().AddForce(gunSpeed * spawnPoint.forward);

        gunSoundPlayer.pitch = Random.Range(0.5f, 1f);
        gunSoundPlayer.PlayOneShot(gunSound);
        Destroy(tempBullet, 5);
    }


}