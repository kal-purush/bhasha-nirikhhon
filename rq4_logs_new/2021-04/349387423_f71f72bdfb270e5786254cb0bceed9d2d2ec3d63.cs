//Suryani
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Bullet : MonoBehaviour
{
    [SerializeField] private float despawnDistance = 1f;
    Vector3 originalPos;

    // Start is called before the first frame update
    void Start()
    {
        originalPos = new Vector3(transform.position.x, transform.position.y, transform.position.z);
    }

    private void OnCollisionExit(Collision collision)
    {
        Destroy(this.gameObject);
    }

    // Update is called once per frame
    void Update()
    {
        if ((transform.position - originalPos).magnitude > despawnDistance)
        {
            Destroy(this.gameObject);
        }
    }
}