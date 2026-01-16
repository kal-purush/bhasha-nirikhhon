using System.Collections;
using System.Collections.Generic;
using Assets.Scripts.Combat;
using UnityEngine;

[RequireComponent(typeof(Collider2D))]
public class CollsionDamage : MonoBehaviour
{
    [SerializeField] private int _damagePerTick;
    [SerializeField] private Vector2 _knockbackFactor = Vector2.one;
    [SerializeField] private bool _useTrigger = false;
    [SerializeField] private bool _damagePlayer = true;
    [SerializeField] private bool _damageEnemy = false;

    void OnCollisionStay2D(Collision2D collision)
    {
        HandleCollide(collision.gameObject);
    }

    void OnTriggerStay2D(Collider2D other)
    {
        if (!_useTrigger) return;
        HandleCollide(other.gameObject);
    }

    private void HandleCollide(GameObject colliding)
    {
        bool isPlayer = colliding == PlayerManager.Instance.gameObject;

        if (
            ((isPlayer && _damagePlayer) || (!isPlayer && _damageEnemy)) &&
            colliding.TryGetComponent(out Health health)
        )
        {
            var hitDirectiion = Vector2.Scale(
                colliding.transform.position - transform.position,
                _knockbackFactor
            ).normalized;
            Debug.DrawRay(transform.position, hitDirectiion, Color.red, 1f);
            health.TakeDamage(_damagePerTick, hitDirectiion, _knockbackFactor.magnitude);
        }
    }
}