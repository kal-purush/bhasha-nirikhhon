using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DamageAllEnemies : ShootEnemies
{
    public int damage;
    public AudioClip shootSFX;

    public GameObject particleEffect;

    public override void ShootTarget()
    {
        AudioSource.PlayClipAtPoint(shootSFX, transform.position);
        foreach (GameObject enemy in enemiesInRange)
        {
            if (particleEffect != null)
            {
                GameObject effect = Instantiate(particleEffect, enemy.transform.position + Vector3.up, Quaternion.identity);
                effect.transform.SetParent(enemy.transform);
                Destroy(effect, 0.5f);
            }
            enemy.GetComponent<EnemyBehavior>().TakeDamage(damage);
        }
    }

    public override void UpgradeDamage(int newDamage)
    {
        damage = newDamage;
    }
}