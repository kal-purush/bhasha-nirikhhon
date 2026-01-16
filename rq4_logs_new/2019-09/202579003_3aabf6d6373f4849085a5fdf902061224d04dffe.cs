using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AbilityA : BaseBossAbility
{
    public GameObject projectileA;
    public Vector2 direction;
    public float speed;

    public override void Trigger(Vector2 _direction)
    {
        base.Trigger(_direction);
        //LaunchAbilityAProjectile(_direction);
        NotifyAbilityCompleted();
    }

    private void LaunchAbilityAProjectile(Vector2 _direction)
    {
        var baseEnemyProj = Instantiate(projectileA, this.transform.position, Quaternion.identity).GetComponent<Projectiles.BaseProjectile>();
        baseEnemyProj.GetComponent<Rigidbody2D>().velocity = _direction * speed;
    }
}

