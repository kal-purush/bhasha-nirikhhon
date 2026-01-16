using UnityEngine;
using System.Collections;

[RequireComponent(typeof(LineRenderer))]

public class LaserGun : MonoBehaviour
{
    LineRenderer line;

    void Start()
    {
        line = GetComponent<LineRenderer>();
        line.enabled = false;

        Cursor.lockState = CursorLockMode.Locked;
    }

    void Update()
    {
        if(Input.GetMouseButtonDown(0)) {
            StopCoroutine("ShootLaser");
            StartCoroutine("ShootLaser");
        }
    }

    IEnumerator ShootLaser()
    {
        line.enabled = true;

        while(Input.GetMouseButtonDown(0))
        {
            Ray ray = new Ray(transform.position, transform.forward);
            RaycastHit hit;


            line.SetPosition(0, ray.origin);

            if(Physics.Raycast(ray, out hit, 100))
            {
                line.SetPosition(1, hit.point);
                if(hit.rigidbody)
                {
                    hit.rigidbody.AddForceAtPosition(transform.forward * 1000, hit.point);
                }
            }
            else
            {
                line.SetPosition(1, ray.GetPoint(100));
            }

            yield return null;
        }

        line.enabled = false;
    }
}