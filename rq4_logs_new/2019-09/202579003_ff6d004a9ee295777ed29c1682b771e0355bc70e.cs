using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ProjectileB_Primary : MonoBehaviour
{
    public List<Transform> pointsForChildProjectils;
    public GameObject childProjectile;
    public float child_speed;
    public float child_damage;

    public void BlastProjectileToReleaseSecondaryProjectile()
    {
        for(int i0 = 0; i0 < pointsForChildProjectils.Count; i0++)
        {
            var baseEnemyProj = Instantiate(childProjectile, pointsForChildProjectils[i0].position, Quaternion.identity).GetComponent<Projectiles.BaseProjectile>();
            baseEnemyProj.GetComponent<Common.AffectorAmount>().SetDamage(child_damage);
            baseEnemyProj.GetComponent<Rigidbody2D>().velocity = this.transform.forward * child_speed;
        }
    }
}