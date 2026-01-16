using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyProjectile : MonoBehaviour
{
    private EnemyLogic enemyLogic;
    private Vector2 direction;
    private float speed = 25f;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        transform.position += (Vector3)direction * (speed * Time.smoothDeltaTime);
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            Player player = collision.gameObject.GetComponent<Player>();
            player.DamagePlayer(enemyLogic.projDmg);
            Destroy(gameObject);

            Knockback knockback = collision.gameObject.GetComponent<Knockback>();
            knockback.PlayFeedback(gameObject);
        }
    }

    private void OnBecameInvisible()
    {
        Destroy(gameObject);
    }

    public void Direction(Vector2 dir)
    {
        direction = dir.normalized;
    }

    public void setOwner(GameObject enemy)
    {
        enemyLogic = enemy.GetComponent<EnemyLogic>();
    }
}