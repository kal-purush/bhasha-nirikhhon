using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShootProjectile : MonoBehaviour {

    float damage;
    float speed;
    Transform direction;
    string tagOfTarget;

    GameObject projectile;

    // Use this for initialization
    void Start () {
        projectile = GetComponent<GameObject>();
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void SetDamage(float damage)
    {
        this.damage = damage;
    }

    public void SetSpeed(float speed)
    {
        this.speed = speed;
    }

    public void Shoot(Transform direction)
    {
        GameObject clone = GameObject.Instantiate(projectile);
    }

    public void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.gameObject.tag == tagOfTarget)
        {
            collision.gameObject.GetComponent<Entity>().ModifyHealth(damage);
        }
        Destroy(GetComponent<GameObject>());
    }
}