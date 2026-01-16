using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Health : MonoBehaviour {
    Hero hero;
    private float LastDamage = -1;
    private float invTime = 1f;
    // Use this for initialization
    void Start () {
        Hero hero = GetComponent<Hero>();
    }
	
	// Update is called once per frame
	void Update () {
		
	}
    /*
    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (Time.time - LastDamage > invTime)
        {
            GameObject GO = collision.gameObject;
            print("collison: " + GO.tag +" "+ GO.name);
            if (GO.tag.Equals("Enemy") || GO.tag.Equals("Enemy Bullet"))
            {
                Hero hero = GetComponent<Hero>();
                hero.damage(collision.gameObject.GetComponent<DamageComponent>().damage);
                LastDamage = Time.time;
            }
        }
    }
    */
    private void OnCollisionStay2D(Collision2D collision)
    {
        if (Time.time - LastDamage > invTime)
        {
            GameObject GO = collision.gameObject;
            //print("collison: " + GO.tag + " " + GO.name);
            if (GO.tag.Equals("Enemy") || GO.tag.Equals("Enemy Bullet"))
            {
                Hero hero = GetComponent<Hero>();
                hero.damage(collision.gameObject.GetComponent<DamageComponent>().damage);
                LastDamage = Time.time;
            }
        }
    }
}