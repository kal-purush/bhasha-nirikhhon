using UnityEngine;
using System.Collections;

public class AI_Mob : MonoBehaviour {
    // AI VARIABLES:
    public float MoveSpeed = 0.5f;
    public float ChaseDistance = 3.0f;
    public float AttackDistance = 1.0f;
    public float waitTime = 0.5f;
    private float DistanceToPlayer;

    private GameObject Player;
    public GameObject projectile;

    // START:
    void Start() {
        Player = GameObject.FindGameObjectWithTag("Player");
    } // END START()

    // UPDATE:
    void Update() {
        DistanceToPlayer = Vector2.Distance(transform.position, Player.transform.position);

        //  If the distance between the mob and the player
        //  is less than or equal to the ChaseDistance,
        //  and greater than the AttackDistance,
        //  the mob moves towards the player.
        if (DistanceToPlayer <= ChaseDistance && DistanceToPlayer > AttackDistance) {
            transform.position = Vector2.MoveTowards(transform.position, Player.transform.position, MoveSpeed * Time.deltaTime);
        }

        //  If the distance between the mob and the player
        //  is less than or equal to the AttackDistance,
        //  the mob launches a projectile at the player.
        if (DistanceToPlayer <= AttackDistance) {
            Debug.Log(this.gameObject.name + " is now in range! Ready to shoot player!");
            transform.position = transform.position;

            waitTime -= Time.deltaTime;
            if (waitTime <= 0) {
                Instantiate(projectile, transform.position, Quaternion.identity);
                waitTime = 0.5f;
            }
        }
    } // END UPDATE()
} // END CLASS AI_Mob