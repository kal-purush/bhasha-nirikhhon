using Assets.Scripts.Combat;
using UnityEngine;

public class Pulse : MonoBehaviour
{
    [SerializeField] private float _increaseScale;

    private void Start()
    {
        transform.localScale = Vector3.zero;
        Destroy(gameObject, 3f);
    }

    private void Update()
    {
        transform.localScale += Vector3.one * _increaseScale;
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision != null && !collision.CompareTag("Player"))
        {
            Debug.Log(collision.name);
            if (collision.TryGetComponent(out Health health))
            {
                /*Destroy(collision.gameObject);*/
                health.TakeDamage(30, collision.transform.position - transform.position, 1f);
            }
            else if (collision.TryGetComponent(out Bullet bullet))
            {
                Destroy(collision.gameObject);
            }
        }
    }


}