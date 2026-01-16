using UnityEngine;

public class PlayerProjectile : MonoBehaviour
{
    public float lifetime = 4f;
    public float damage = 1f;

    private void Start()
    {
        Destroy(gameObject, lifetime);
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        if (other.CompareTag("Boss"))
        {
            other.GetComponent<BossController>().TakeDamage();
            Destroy(gameObject);
        }
        else if (!other.CompareTag("Player"))
        {
            Destroy(gameObject);
        }
    }
}