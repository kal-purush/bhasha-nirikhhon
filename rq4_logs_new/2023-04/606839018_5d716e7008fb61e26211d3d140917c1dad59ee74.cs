using System;
using UnityEngine;

public class BluntWeapon : BaseParamAcceptingEntity
{
    [SerializeField] private BluntParams details;
    private Vector3 topPoint;
    private Vector3 bottomPoint;

    public override void ApplyParams(AbilityParam generalParam)
    {
    }

    public override AbilityParam GetParams()
    {
        return details;
    }

    private void Start()
    {
        CalculateCapsuleBorders();
    }

    private void CalculateCapsuleBorders()
    {
        topPoint = transform.up * details.length / 2;
        bottomPoint = -transform.up * details.length / 2;
    }

    private void LateUpdate()
    {
        var colliders = Physics.OverlapCapsule(transform.position + topPoint, transform.position + bottomPoint,
            details.radius, details.detectionMask);
        if (colliders.Length <= 0) return;


        foreach (var collider in colliders)
        {
            CheckForAffectedEntityAndApply(collider);
        }
    }

    private void CheckForAffectedEntityAndApply(Collider collider)
    {
        if (collider.CompareTag("Player"))
        {
            EventStore.Instance.PublishPlayerAbilityAffected(GetParams());
            return;
        }

        IAbilityAffected entity;
        if (collider.TryGetComponent(out entity)
            || (collider.transform.parent != null && collider.transform.parent.TryGetComponent(out entity)))
        {
            entity.ApplyAbility(details);
        }
    }

    private void OnDrawGizmosSelected()
    {
        CalculateCapsuleBorders();
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position + topPoint, details.radius);
        Gizmos.DrawWireSphere(transform.position + bottomPoint, details.radius);
    }
}

[Serializable]
public class BluntParams : AbilityParam
{
    public LayerMask detectionMask;
    public float length;
    public float radius;
}