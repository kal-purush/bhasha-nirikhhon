using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Health : MonoBehaviour {
    private Hero hero;
	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}
    private void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Enemy")
        {
            hero.damage(10f);
        }
        if (other.tag == "Enemy Projectiles")
        {
            hero.damage(other.GetComponent<DamageComponent>().damage);
        }
    }

    internal void setHero(Hero hero)
    {
        this.hero = hero;
    }
}