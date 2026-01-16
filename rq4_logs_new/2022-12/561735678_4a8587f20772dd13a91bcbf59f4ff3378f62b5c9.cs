using System.Collections.Generic;
using UnityEngine;

public class DistantWeapon : AbstractWeapon
{
    public float timeout = 1.0f;

    public Projectile projectile;

    public float spawnDistance;
    
    public Vector3 spawnOffset;

    protected PlayerWeaponEnchanter playerWeaponEnchanter;
    
    private List<CreatureEffectManager> _enemyList;

    private float _lastActivationTime;
    
    private void Start()
    {
        _enemyList = new List<CreatureEffectManager>();

        playerWeaponEnchanter = GetComponentInParent<PlayerWeaponEnchanter>();

        _lastActivationTime = Time.time;
    }

    public override bool Activate(Vector3 direction)
    {
        if (_lastActivationTime + playerWeaponEnchanter.EnchantTimeout(timeout) > Time.time)
        {
            return false;
        }

        Damage(direction);

        _lastActivationTime = Time.time;

        return false;
    }

    public virtual void Damage(Vector3 direction)
    {
        Projectile spawnedProjectile = Instantiate(
            projectile,
            transform.position + spawnOffset + direction * spawnDistance,
            Quaternion.FromToRotation(Vector3.right, direction));
        
        spawnedProjectile.damage = playerWeaponEnchanter.EnchantDamage(spawnedProjectile.damage, false);
        spawnedProjectile.direction = direction;
        
        // playerWeaponEnchanter.AdditionalEnemyAttack(enemy);
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.isTrigger)
        {
            return;
        }
        
        if (collision.CompareTag("Enemy"))
        {
            _enemyList.Add(collision.GetComponent<CreatureEffectManager>());
        }
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.isTrigger)
        {
            return;
        }
        
        if (collision.CompareTag("Enemy"))
        {
            _enemyList.Remove(collision.GetComponent<CreatureEffectManager>());
        }
    }
}