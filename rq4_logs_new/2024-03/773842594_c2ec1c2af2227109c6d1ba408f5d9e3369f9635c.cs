using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Arrow : MonoBehaviour
{
    [SerializeField][ReadOnlyInspector] float z;
    [SerializeField][ReadOnlyInspector] bool lateStart;

    private void Update()
    {
        if(lateStart == false)
        {
            z = transform.localEulerAngles.z;
            lateStart = true;
        }

        transform.rotation = Quaternion.Euler(0, 0, z);


    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.tag == "DotCircle")
        {
            z -= 5;
        }

        if (collision.gameObject.tag == "Wall" || collision.gameObject.tag == "Ground")
        {
            this.transform.GetComponent<Rigidbody2D>().bodyType = RigidbodyType2D.Static;
        }
    }
}