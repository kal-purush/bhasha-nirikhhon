
using System;
using UnityEngine;

public class Chair : MonoBehaviour
{
    private bool inRange = false;
    private bool sit = false;
    void Update()
    {
        Player p = Player.instance;
        if (inRange && Input.GetKeyDown(KeyCode.R))
        {
            Debug.Log("Sit");
            p.speed = 0f;
            p.GetComponent<Rigidbody>().useGravity = false;
            p.transform.position = transform.position - new Vector3(0f, 0f, 0.5f);
            sit = true;
        }

        if (sit)
        {
            p.transform.position = transform.position - new Vector3(0f, 0f, 0.5f);
        }
    }

    void OnTriggerEnter(Collider other)
    {
        var player = other.GetComponent<Player>();
        if (player != null)
        {
            inRange = true;
        }
    }

    private void OnTriggerExit(Collider other)
    {
        var player = other.GetComponent<Player>();
        if (player != null)
        {
            inRange = false;
        }
    }
}