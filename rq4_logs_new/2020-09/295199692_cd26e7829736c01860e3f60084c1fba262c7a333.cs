using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AntiGravityField : MonoBehaviour
{
    public float slowdownSpeed = 0.5f;

    private List<Rigidbody2D> objectsInField;

    void Start()
    {
        objectsInField = new List<Rigidbody2D>();
    }

    void Update()
    {
        foreach(var obj in objectsInField)
        {
            obj.velocity = new Vector2(obj.velocity.x, Mathf.Lerp(obj.velocity.y, 0, slowdownSpeed * Time.deltaTime));
        }
    }

    void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.gameObject.CompareTag("Player")
            || collision.gameObject.CompareTag("Hazard"))
        {
            return;
        }

        var gravityObject = collision.GetComponent<IGravityObject>();
        if (gravityObject == null)
        {
            return;
        }

        gravityObject.DisableGravity();
        objectsInField.Add(collision.GetComponent<Rigidbody2D>());
    }

    void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.gameObject.CompareTag("Player")
            || collision.gameObject.CompareTag("Hazard"))
        {
            return;
        }

        var gravityObject = collision.GetComponent<IGravityObject>();
        if (gravityObject == null)
        {
            return;
        }

        gravityObject.EnableGravity();
        objectsInField.Remove(collision.GetComponent<Rigidbody2D>());
    }
}