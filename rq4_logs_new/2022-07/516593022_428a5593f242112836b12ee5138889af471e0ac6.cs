using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RangeAttack : MonoBehaviour {
	public float fireDealy, speedUp, speed, spread;
    private float trueSpeed;
	public int damage;
    private int playerInSightTimer;
	private bool canFire = true, speedUpLode = false;
	public GameObject ammo,spawnPoint;
	private RotaeTowars RotaeScript;

	// Use this for initialization
	void Start () {
        trueSpeed = speed;
		RotaeScript = this.GetComponent<RotaeTowars>();
        StartCoroutine(outPut());
    }
	
	// Update is called once per frame
	void Update () {

        playerInSight();

        if (RotaeScript.ReadyToAttack() && canFire){

			FireBullet();
			StartCoroutine(FireTimer());
		} 
	}

    private void playerInSight()
    {
        if(RotaeScript.ReadyToAttack() && speedUpLode == false)
        {
            speedUpLode = true;
            StartCoroutine(inSight());
        }
        if(RotaeScript.ReadyToAttack() == false)
        {
            speed = trueSpeed;
        }
    }

	private void FireBullet()
    {
        Quaternion rotation = spawnPoint.transform.rotation;
        GameObject ammoObj = Instantiate(ammo, spawnPoint.transform.position, rotation);
        ammoObj.GetComponent<Rigidbody>().AddForce(transform.forward * speed, ForceMode.Force);
	}

	private IEnumerator FireTimer()
    {
        canFire = false;
        yield return new WaitForSeconds (fireDealy);
        canFire = true;
    }

    private IEnumerator inSight()
    {
        yield return new WaitForSeconds(1);
        speed += speedUp;
        speedUpLode = false;
    }

    private IEnumerator outPut()
    {
        yield return new WaitForSeconds(1);
        print("the ammo speep ia at " + speed);
        StartCoroutine(outPut());
    }
}