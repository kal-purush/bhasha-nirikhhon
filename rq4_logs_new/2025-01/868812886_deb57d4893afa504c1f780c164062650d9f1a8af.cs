using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Difficulty : MonoBehaviour
{
    private PlayerHealth _health;
    private HealthPickup bonus;
    private GunSystem gun;
    private EnemyHealth _enemyHealth;
    private BaseEnemy _enemy;

    public void easy()
    {
        bonus.healthBonus = 20;
        gun.totalAmmo = 60;
        gun.damage = 15;
        _enemyHealth.maxHealth = 90;
        _enemy.attackDamage1 = 8;

    }


    public void medium()
    {
        
    }


    public void hard()
    {
        bonus.healthBonus = 5;
        gun.totalAmmo = 40;
        gun.damage = 10;
        _enemyHealth.maxHealth = 110;
        _enemy.attackDamage1 = 15;
        
    }
}