using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class NPCRangedHitbox : MonoBehaviour, IDamageable
{
    [SerializeField] private int health = 3;
    [SerializeField] private bool invulnerable = false;
    [SerializeField] private bool targetable = true;
    [SerializeField] private int damage = 1;
    [SerializeField] private GameObject parent;
    [SerializeField] private AnimatorBrain animateSprite;
    [SerializeField] private RangedMovement movement;
    public int Health { get => health; set => health = value; }
    public bool Invulnerable { get => invulnerable; set => invulnerable = value; }
    public bool Targetable { get => targetable; set => targetable = value; }

    void Awake()
    {
        parent = gameObject.transform.parent.gameObject;
        animateSprite = gameObject.GetComponentInParent<AnimatorBrain>();
        movement = gameObject.GetComponentInParent<RangedMovement>();
    }

    void Update()
    {
        if (health <= 0)
        {
            movement.Immobilize();
            animateSprite.OnDeath();
        }
    }

    void OnEnable()
    {
        animateSprite.OnDeathEvent += OnObjectDestroyed;
    }
    void OnDisable()
    {
        animateSprite.OnDeathEvent -= OnObjectDestroyed;
    }

    public void OnHit(int damage, Vector2 knockback)
    {
        if (knockback != Vector2.zero)
        {
            gameObject.GetComponentInParent<Rigidbody2D>().AddForce(knockback, ForceMode2D.Impulse);
        }

        if (!invulnerable)
        {
            health -= damage;
            if (health <= 0)
            {
                movement.Immobilize();
                animateSprite.OnDeath();
            }
            else
            {
                animateSprite.OnHit();
            }
        }
    }
    public void OnHit(int damage)
    {
        if (!invulnerable)
        {
            health -= damage;
            if (health <= 0)
            {
                movement.Immobilize();
                animateSprite.OnDeath();
            }
            else
            {
                animateSprite.OnHit();
            }
        }
    }

    public void OnObjectDestroyed()
    {
        Destroy(parent);
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.gameObject.CompareTag("PlayerHitbox"))
        {
            other.GetComponent<IDamageable>().OnHit(damage);
        }

        if (other.gameObject.CompareTag("Attack"))
        {
            Vector2 _kb = GetKnockbackDirection(other);
            int _damage = GetDamage(other);

            OnHit(_damage, _kb);
        }
    }

    private void OnTriggerStay2D(Collider2D other)
    {
        if (other.gameObject.CompareTag("PlayerHitbox"))
        {
            other.GetComponent<IDamageable>().OnHit(damage);
        }
    }

    private Vector2 GetKnockbackDirection(Collider2D other)
    {
        Vector2 _kbOther = other.GetComponent<IAttack>().KnockbackForce;
        Vector2 _kb = new Vector2(Mathf.Sign(transform.position.x - other.transform.position.x) * _kbOther.x, Math.Sign(transform.position.y - other.transform.position.y) * _kbOther.y).normalized;

        return _kb;
    }

    private int GetDamage(Collider2D other)
    {
        return other.GetComponent<IAttack>().Damage;
    }
}