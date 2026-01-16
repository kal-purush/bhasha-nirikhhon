using UnityEngine;
using System.Collections;
using UnityEngine.SocialPlatforms;

public class EnemyController : MonoBehaviour {

    public float speed = 50;
    public float rangeAttack = 50;
    private Rigidbody2D rigi;
    public GameObject player;
    public bool isFacingRight = true;

    void Start()
    {
        rigi = GetComponent<Rigidbody2D>();
    }

    void FixedUpdate()
    {
        float dX = player.transform.position.x - transform.position.x;
        float dY = player.transform.position.y - transform.position.y;
        float d = Mathf.Sqrt(Mathf.Pow(dX, 2) + Mathf.Pow(dY, 2));
        if (d < rangeAttack && d > 1)
        {
            rigi.AddForce(new Vector2(
                Mathf.Sign(dX)* speed* (1-Mathf.Abs(dX)/ rangeAttack), 
                Mathf.Sign(dY) * speed/2 * (1-Mathf.Abs(dY) / rangeAttack)));
        }
        if (rigi.velocity.x != 0 || rigi.velocity.y != 0) rigi.AddForce(new Vector2(-rigi.velocity.x * 10, -rigi.velocity.y * 10));
        if (rigi.velocity.x > 0 && !isFacingRight)
            Flip();
        else if (rigi.velocity.x < 0 && isFacingRight)
            Flip();
    }

    private void Flip()
    {
        isFacingRight = !isFacingRight;
        Vector3 theScale = transform.localScale;
        theScale.x *= -1;
        transform.localScale = theScale;
    }

    void OnTriggerEnter2D(Collider2D other)
    {
    }
}