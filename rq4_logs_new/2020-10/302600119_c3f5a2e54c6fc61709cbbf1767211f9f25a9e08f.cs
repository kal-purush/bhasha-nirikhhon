using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class grabSurfaceScript : MonoBehaviour
{
    [SerializeField]
    public float distance = 0f;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        watchSurroundings();
        Debug.Log(distance);
    }

    void watchSurroundings()
    {
        Vector3 direction = new Vector3(transform.position.x, transform.position.y -1, transform.position.z) - transform.position;
        RaycastHit2D hit = Physics2D.Raycast(transform.position, direction, 50f);
        distance = Vector2.Distance(transform.position, hit.point);
        Debug.DrawRay(transform.position, direction * 20f);
    }
}