using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyHitbox : Collideable
{
    public int damage;
    public float pushForce;

    protected override void OnCollide(Collider2D coll)
    {
        if (coll.tag == "Fighter" && coll.name == "Player_01")
        {
            //Create a damage object and use it to send it to player (or what has been hit).
            Damage dmg = new Damage
            {
                damageAmount = damage,
                origin = transform.position,
                pushForce = pushForce
            };

            coll.SendMessage("ReceiveDamage", dmg);
        }
    }
}