using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PredictedProjectile : MonoBehaviour
{

    public Rigidbody bullet;
    public GameObject predictedPosition;
    public LayerMask layer;

    private Camera cam;
    // Use this for initialization
    void Start()
    {
        cam = Camera.main;
    }

    // Update is called once per frame
    void Update()
    {

        fireProjectile();

    }

    void fireProjectile()
    {
        Ray camRay = cam.ScreenPointToRay(Input.mousePosition);
        RaycastHit hit;

        if (Physics.Raycast(camRay, out hit, 100f, layer))
        {
            Debug.Log("true");
            predictedPosition.SetActive(true);
            predictedPosition.transform.position = hit.point + Vector3.up * 0.1f;
        }
    }

    Vector3 CalculateVelocity(Vector3 target, Vector3 origin, float time)
    {
        Vector3 distance = target - origin;
        Vector3 distanceXZ = distance;
        distanceXZ.y = 0f;

        float Sy = distance.y;
        float Sxz = distanceXZ.magnitude;

        float Vxz = Sxz / time;
        float Vy = Sy / time + 0.5f * Mathf.Abs(Physics.gravity.y) * time;

        Vector3 result = distanceXZ.normalized;
        result *= Vxz;
        result.y = Vy;

        return result;
    }
    }
