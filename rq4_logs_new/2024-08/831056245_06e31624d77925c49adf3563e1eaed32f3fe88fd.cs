using UnityEngine;

public class ShakeDetector : MonoBehaviour
{
    public float shakeThreshold = 0.5f;  // The threshold for detecting shake.
    private Vector3 previousPosition;
    private float shakeAmount;

    void Start()
    {
        previousPosition = transform.position;
    }

    void Update()
    {
        Vector3 movement = transform.position - previousPosition;
        shakeAmount = movement.magnitude / Time.deltaTime;
        previousPosition = transform.position;

        if (shakeAmount > shakeThreshold)
        {
            OnShakeDetected();
        } else {
            GetComponent<Renderer>().material.color = Color.white;
        }
    }

    void OnShakeDetected()
    {
        Renderer renderer = GetComponent<Renderer>();
        Debug.Log("Shake detected!");
        renderer.material.color = Color.red;

    }
}
