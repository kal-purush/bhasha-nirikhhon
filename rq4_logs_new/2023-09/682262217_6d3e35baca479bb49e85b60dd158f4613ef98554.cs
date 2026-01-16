using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MiningGun : MonoBehaviour
{
    public Transform laserSpawnPoint;
    public float maxCharge;
    public float currentCharge;
    public float laserStrength;
    //public LineLaser laser;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void ShootMiningLaser()
    {

    }

    public void MiningLaserRecharge()
    {

    }

    public void MiningGunVacuumMode()
    {

    }

    private IEnumerator FiringMode()
    {
        yield return new WaitForSeconds(1f);
    }
}