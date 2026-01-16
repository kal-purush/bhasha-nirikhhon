using UnityEngine;
using System.Collections;

public class Inkwizytor : MonoBehaviour 
{
    public float speed = 3f;
    private Vector3 movement;
    Rigidbody playerRigidbody;

    void Awake()
    {
        playerRigidbody = GetComponent<Rigidbody>();
    }
    void FixedUpdate()
    {
        float h = Input.GetAxisRaw("Horizontal");
        float v = Input.GetAxisRaw("Vertical");
        Move(h, v);
    }
    void Move(float h, float v)
    {
        movement.Set(h, 0f, v);
        movement = movement.normalized * (-speed) * Time.deltaTime;
        playerRigidbody.MovePosition(transform.position + movement);
    }
    void OnCollisionStay(Collision collision)
    {
        if(collision.gameObject.tag == "Przedmiot")
        {
            Debug.Log("colisja", collision.gameObject);
            if (Input.GetKeyDown(KeyCode.Space))
            {
                collision.gameObject.GetComponentInParent<Przedmioty>().zmiana();
            }
        }
    }
}