using UnityEngine;

public class Fireball : MonoBehaviour
{
    private new Rigidbody2D rb;
    private Vector2 velocity;
    private Vector2 direction;

    public float speed = 12;
    public float bounceForce = 6;
    public float gravityModifier = 4;
    public float terminalVelocity = -15;

    private Player player;

    private void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        player = GameObject.FindWithTag("Player").GetComponent<Player>();
        direction = transform.rotation.y == 0 ? Vector2.right : Vector2.left;
    }

    private void FixedUpdate()
    {
        velocity.x = speed * direction.x;
        velocity.y += Physics2D.gravity.y * Time.fixedDeltaTime * gravityModifier;
        velocity.y = Mathf.Max(velocity.y, terminalVelocity);
        rb.MovePosition(rb.position + velocity * Time.fixedDeltaTime);

        if (rb.Raycast(Vector2.down, 0.1f))
        {
            velocity.y = bounceForce;
        }
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (!collision.gameObject.CompareTag("Enemy") && rb.Raycast(direction, 0.375f, 0.1f) && !rb.Raycast(-direction, 0.375f, 0.1f))
        {
            // Destroying the gameobject activates OnBecameInvisible
            Destroy(gameObject);
        }
    }

    private void OnBecameInvisible()
    {
        player.fireballCount--;
        if (gameObject != null)
        {
            Destroy(gameObject);
        }
    }
}