using UnityEngine;
using System.Collections;
using UnityEditor;

public class SimpleEnemyScripy : MonoBehaviour
{

    public float velocity = 1f;

    public Transform sightStart;
    public Transform sightEnd;
    private new Rigidbody2D rigidbody;
    public LayerMask detectWhat;
    public bool colliding;

    Animator anim;
    private void Awake()
    {
        rigidbody = GetComponent<Rigidbody2D>();
        enabled = false;
    }


    // Use this for initialization
    void Start()
    {
        //anim = GetComponent<Animator>();
        Physics2D.queriesStartInColliders = true;
    }

    // Update is called once per frame
    void Update()
    {
        if (enabled)
        {
            GetComponent<Rigidbody2D>().velocity = new Vector2(velocity, GetComponent<Rigidbody2D>().velocity.y);
        }
        colliding = Physics2D.Linecast(sightStart.position, sightEnd.position, detectWhat);

        if (colliding)
        {

            transform.localScale = new Vector2(transform.localScale.x * -1, transform.localScale.y);
            velocity *= -1;

        }

    }

    void OnDrawGizmos()
    {
        Gizmos.color = Color.magenta;

        Gizmos.DrawLine(sightStart.position, sightEnd.position);
    }

    private void OnBecameVisible()
    {
        enabled = true;
    }

    private void OnBecameInvisible()
    {
        enabled = false;
    }

    private void OnEnable()
    {
        rigidbody.WakeUp();
    }

    private void OnDisable()
    {
        GetComponent<Rigidbody2D>().velocity = new Vector2(velocity, GetComponent<Rigidbody2D>().velocity.y*0);
        rigidbody.velocity = Vector2.zero;
        rigidbody.Sleep();
    }

}