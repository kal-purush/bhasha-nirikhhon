using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SaveChickenFallen_Y : MonoBehaviour
{
    public float lowerLimit;
    private Vector3 respawnPos;

    // Start is called before the first frame update
    void Start()
    {
        respawnPos = transform.position;
    }

    // Update is called once per frame
    void Update()
    {
        if (transform.position.y <= lowerLimit)
        {
            GetComponent<Rigidbody>().velocity = Vector3.zero;
            transform.position = respawnPos;
        }
    }
}