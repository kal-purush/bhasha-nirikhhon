using UnityEngine;
using System.Collections;

public class AttackWave : MonoBehaviour
{
    private Rigidbody2D rigidBody;

    void Awake()
    {
        rigidBody = GetComponent<Rigidbody2D>();
    }

    void Start()
    {
        rigidBody.velocity = transform.right * 5;

        StartCoroutine(StartDestroyAfterTime(.5f));
    }

    IEnumerator StartDestroyAfterTime(float time)
    {
        yield return new WaitForSeconds(time);

        Destroy(this.gameObject);
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        var zombie = other.gameObject.GetComponent<Zombie>();

        if (zombie != null)
        {
            Debug.Log(other.gameObject);
            zombie.GetHit();
            Destroy(gameObject, 0.1f);
        }
    }
}