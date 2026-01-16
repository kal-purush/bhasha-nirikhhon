using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MagicBurstHitbox : MonoBehaviour
{
    public float lifeSpan = 2f;
    public float increaseSpeed = 2f;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.gameObject.tag == "Enemy")
        {
            gameObject.GetComponent<MeleeEnemyController>().TakeDamage(100, transform.position, 10);
        }
    }

    // Update is called once per frame
    void Update()
    {
        transform.localScale *= increaseSpeed * Time.deltaTime;
    }
}