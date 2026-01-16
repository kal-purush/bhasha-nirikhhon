using UnityEngine;

public class BulletController : MonoBehaviour
{
    public int Direction;
    public float Speed;
    public int Damage;    

    private void Update()
    {
        Move();
    }

    private void OnTriggerEnter2D(Collider2D other)
    {
        var temp = other.gameObject.GetComponent<EnemyController>();
        if (temp != null)
        {
            temp.Health -= Damage;
            Destroy(gameObject);
        }        
    }

    void Move()
    {
        transform.position += Vector3.right * Direction * Speed;
    }

    private void OnBecameInvisible()
    {
        Destroy(gameObject, 2f);
    }
}