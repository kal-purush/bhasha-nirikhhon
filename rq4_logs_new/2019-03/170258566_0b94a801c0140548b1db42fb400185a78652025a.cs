using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyController : MonoBehaviour
{
    public float maxSpeed;

    public Health hp;
    public Transform corpse;
    public Transform bullet;
    public Transform gunPivot;
    public Transform gun;

    private Rigidbody2D rb2d;
    private Rigidbody2D player;

    private float agroDistance = 20;
    private bool agroed = false;
    private float time = 15;

    
    void Start()
    {
        rb2d = GetComponent<Rigidbody2D>();
        player = GameObject.FindWithTag("Player").GetComponent<Rigidbody2D>();
    }

    void Update()
    {

    }

    void FixedUpdate()
    {
        if (hp.alive)
        {
            Vector2 enemy_vec = new Vector2(rb2d.transform.position.x, rb2d.transform.position.y);
            Vector2 player_vec = new Vector2(player.transform.position.x, player.transform.position.y);
            Vector2 enemy_to_player = player_vec - enemy_vec;
            float distance = Mathf.Abs(Vector2.Distance(enemy_vec, player_vec));
            if (agroed || distance < agroDistance)
            {
                float angle = Mathf.Atan2(enemy_to_player.y, enemy_to_player.x);
                gunPivot.rotation = Quaternion.Euler(0, 0, angle * 180 / Mathf.PI); //aim gun at player
                RaycastHit2D hit = Physics2D.Linecast(gun.position, player_vec); //check if the enemy can see the player
                if (hit.transform.tag == "Player") //If it can see the player, then shoot at it 
                {
                    agroed = true;
                    if (distance > 3)
                    {
                        enemy_to_player.Normalize();
                        rb2d.velocity = enemy_to_player * maxSpeed;
                    }
                    time += 1;
                    if (time > 30)
                    {
                        time = 0;
                        var shooting = Instantiate(bullet, gun.position, gun.rotation);
                        shooting.tag = "EnemyAttack";
                    }
                }
            }
        }
        else
        {
            Instantiate(corpse, new Vector3(rb2d.gameObject.transform.position.x + 1f, rb2d.gameObject.transform.position.y - 0.7f, rb2d.gameObject.transform.position.z), Quaternion.identity);
            Destroy(rb2d.gameObject);
        }
    }
}