using UnityEngine;

public class Dog : MonoBehaviour
{
    public Transform player; // Reference to the player's transform
    public float detectionDistance = 5f; // Distance at which the dog detects the player
    public float runSpeed = 5f; // Speed at which the dog runs away
    public float returnSpeed = 2f; // Speed at which the dog returns to its original position

    private Rigidbody2D rb2d;
    private Vector2 originalPosition;
    private bool isRunningAway = false;

    void Start()
    {
        rb2d = GetComponent<Rigidbody2D>();
        originalPosition = rb2d.position;
    }

    void Update()
    {
        if (player == null) // If player is not assigned, find it by its tag
            player = GameObject.FindGameObjectWithTag("Player").transform;

        if (player != null)
        {
            Vector2 distanceToPlayer = rb2d.position - (Vector2)player.position;

            // If the player is within the detection range, run away from the player
            if (distanceToPlayer.magnitude < detectionDistance)
            {
                Vector2 moveDirection = distanceToPlayer.normalized;
                rb2d.MovePosition(rb2d.position + moveDirection * runSpeed * Time.deltaTime);
                isRunningAway = true;
            }
            else if (isRunningAway)
            {
                // If the player is not close and the dog was running away, return to its original position
                Vector2 moveDirection = (originalPosition - rb2d.position).normalized;
                rb2d.MovePosition(rb2d.position + moveDirection * returnSpeed * Time.deltaTime);

                // If the dog is close to its original position, stop running away
                if (Vector2.Distance(rb2d.position, originalPosition) < 0.1f)
                    isRunningAway = false;
            }
        }
    }
}








