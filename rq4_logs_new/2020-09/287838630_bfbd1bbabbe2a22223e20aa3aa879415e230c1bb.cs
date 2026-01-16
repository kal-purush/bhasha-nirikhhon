using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(Rigidbody))]
[RequireComponent(typeof(Collider))]
public class AnimationDamage : Damage
{
    private Rigidbody m_RigidBody;
    private Collider m_Collider;
    private bool m_DamageDealt = false;

    private void Awake()
    {
        // Init some variables to behave properly
        m_RigidBody = GetComponent<Rigidbody>();
        m_RigidBody.isKinematic = false;
        m_Collider = GetComponent<Collider>();
        m_Collider.isTrigger = false;
    }

    private void OnCollisionEnter(Collision collision)
    {
        m_RigidBody.isKinematic = true;
    }

    private void OnCollisionExit(Collision collision)
    {
        m_DamageDealt = false;
        m_RigidBody.isKinematic = false;
    }

    public override bool DealDamage(Damageable obj)
    {
        if (!obj || m_DamageDealt) return false;
        obj.OnDamage(Dmg);
        m_DamageDealt = true;
        return true;
    }
}