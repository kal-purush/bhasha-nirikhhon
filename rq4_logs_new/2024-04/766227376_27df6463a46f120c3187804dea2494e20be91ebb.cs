using TPS.Characters;
using UnityEngine;

public class ExplosiveEnemy : EnemyController
{
    [SerializeField] private GameObject explosionEffect;
    [SerializeField] private int damageAmount = 20;

    private bool facingRight = true;

    protected override void Awake()
    {
        base.Awake();

        HealthHandler.OnDeath.AddListener(Death);
    }

    protected override void Update()
    {
        base.Update();

        if (target != null)
        {
            if (target.position.x < transform.position.x && facingRight)
            {
                Flip();
            }
            else if (target.position.x > transform.position.x && !facingRight)
            {
                Flip();
            }
        }
    }

    void Death()
    {
        if (explosionEffect != null)
        {
            Instantiate(explosionEffect, transform.position, Quaternion.identity);
        }
    }

    void Flip()
    {
        facingRight = !facingRight;
        Vector3 theScale = transform.localScale;
        theScale.x *= -1;
        transform.localScale = theScale;
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.collider.CompareTag("Player"))
        {
            if (collision.collider.TryGetComponent<CharacterHealthHandler>(out var healthHandler))
            {
                healthHandler.Damage(damageAmount);
            }
        }
    }
}