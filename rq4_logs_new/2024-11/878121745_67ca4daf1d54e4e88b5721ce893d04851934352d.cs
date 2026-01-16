using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GravSwap : MonoBehaviour
{
    public GameObject Scene;
    private Quaternion targetRotation;
    private bool isRotating = false;
    private float rotationSpeed = 2.0f; // Adjust for faster/slower rotation

    void Update()
    {
        if (Input.GetKeyUp(KeyCode.Space) && !isRotating)
        {
            Switch();
        }
    }

    IEnumerator RotateToTarget()
    {
        isRotating = true;

        // Gradually rotate until the scene matches the target rotation
        while (Quaternion.Angle(Scene.transform.rotation, targetRotation) > 0.1f)
        {
            Scene.transform.rotation = Quaternion.Lerp(
                Scene.transform.rotation,
                targetRotation,
                Time.deltaTime * rotationSpeed // Interpolation step
            );
            yield return null; // Wait until the next frame
        }

        // Snap to the target rotation to avoid small floating-point errors
        Scene.transform.rotation = targetRotation;
        isRotating = false;
    }

    void Switch()
    {
            // Calculate the target rotation
            Quaternion tilt = Quaternion.Euler(180, 0, 0);
            targetRotation = Scene.transform.rotation * tilt;
            StartCoroutine(RotateToTarget());
    }

    private void OnTriggerStay(Collider other)
    {
        if (other.gameObject.CompareTag("DahTrigger"))
        {
            Switch();
            Destroy(other.gameObject);

        }

    }
}
