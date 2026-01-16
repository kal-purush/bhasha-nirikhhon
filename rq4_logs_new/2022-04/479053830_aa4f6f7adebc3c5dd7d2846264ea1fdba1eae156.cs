using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScriptCameraMovement : MonoBehaviour
{

    [Range(0.1f, 9f)] [SerializeField] private float _speedHorizontal;
    [Range(0.1f, 9f)] [SerializeField] private float _speedVertical;

    private Vector2 _rotationVect = Vector2.zero;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
        _rotationVect.x += _speedHorizontal * Input.GetAxis("Mouse X");
        _rotationVect.y += _speedVertical   * Input.GetAxis("Mouse Y");
        _rotationVect.y  = Mathf.Clamp(_rotationVect.y, -50f, 90f);

        Quaternion xQuart = Quaternion.AngleAxis(_rotationVect.x, Vector3.up);
        Quaternion yQuart = Quaternion.AngleAxis(_rotationVect.y, Vector3.left);

        // Same as transform.localEulerAngles = new Vector3(-rotation.y, rotation.x, 0);
        transform.localRotation = xQuart * yQuart;

    }

    // See Order of Execution for Event Functions for information on FixedUpdate() and Update() related to physics queries
    void FixedUpdate()
    {

        // Bit shift the index of the layer (8) to get a bit mask
        int layerMask = 1 << 8;

        // This would cast rays only against colliders in layer 8.
        // But instead we want to collide against everything except layer 8. The ~ operator does this, it inverts a bitmask.
        layerMask = ~layerMask;

        RaycastHit hit;
        // Does the ray intersect any objects excluding the player layer
        if (Input.GetMouseButton(0)) {

            // TODO: Place Teleport Sphere on Object (to see where you are teleporting to)
            // TODO: Raycast ONLY on the Teleport Sphere Object instead of the Camera itself

            if (Physics.Raycast(transform.position, transform.TransformDirection(Vector3.forward), out hit, Mathf.Infinity, layerMask))
            {


                Debug.DrawRay(transform.position, transform.TransformDirection(Vector3.forward) * hit.distance, Color.yellow);
                Debug.Log("Did Hit");
            }
            else
            {
                Debug.DrawRay(transform.position, transform.TransformDirection(Vector3.forward) * 1000, Color.white);
                Debug.Log("Did not Hit");
            }

        }

    }

}