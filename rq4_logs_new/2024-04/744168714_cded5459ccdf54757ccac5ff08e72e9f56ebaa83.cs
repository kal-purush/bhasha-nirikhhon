using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class KillZone : MonoBehaviour, IAttacker
{
    public Vector3 Position => transform.position;

    public void Attack()
    {
        // throw new System.NotImplementedException();
    }

    public void StopAttack()
    {
        // throw new System.NotImplementedException();
    }

    private void OnTriggerEnter(Collider other)
    {
        FireManager.manager.FireDamageOnCollider(this, other, new DamageInformation(9999999f, 0, DamageType.AdditiveDamage, 0, true));
    }
}