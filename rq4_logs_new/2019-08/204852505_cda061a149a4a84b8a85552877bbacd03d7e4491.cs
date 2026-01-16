using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BulletMovement : MonoBehaviour
{

    void Movement()
    {
        Vector2 _temp;
        _temp = transform.position;
        _temp.y = transform.position.y + (5 * Time.deltaTime);
        transform.position = _temp;
    }

    // Start is called before the first frame update
    void Start()
    {
        
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        gameObject.SetActive(false);
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        Movement();
    }
}