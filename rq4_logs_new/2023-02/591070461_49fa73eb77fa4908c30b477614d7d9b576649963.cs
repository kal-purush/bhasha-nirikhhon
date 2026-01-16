using UnityEngine;

public class EnemyController : MonoBehaviour
{
    public bool patrol;
    public bool goLeft;
    public int distance;
    public int speed;
    Vector3 position;
    Vector3 initialPosition;
    int dir = 1;
    void OnCollisionEnter2D(Collision2D col)
    {
        if (col.gameObject.GetComponent<PlayerController>() != null)
        {
            PlayerController playerController = col.gameObject.GetComponent<PlayerController>();
            playerController.TakeDamage();
        }
    }
    void Awake()
    {
        position = GetComponent<Transform>().position;
        initialPosition = position;
        if (goLeft)
        {
            initialPosition.x = position.x - distance;
            dir = -1;
        }
    }
    void Update()
    {
        if (patrol)
        {
            if (transform.position.x - initialPosition.x > distance || transform.position.x - initialPosition.x < 0)
            {
                transform.Rotate(new Vector3(0, 180, 0));
                dir = -1 * dir;
            }
            transform.position += new Vector3(speed * dir * Time.deltaTime, 0, 0);
        }

        EnemyAnimation();
    }
    void EnemyAnimation()
    {
        if (patrol)
            GetComponent<Animator>().Play("walk");
        else
            GetComponent<Animator>().Play("idle");
    }
}