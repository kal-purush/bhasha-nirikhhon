using UnityEngine;

public class CameraFollow : MonoBehaviour {

    public GameObject player;
    public Vector3 offset = new Vector3(-2.5f, 7f, -2.5f);
    public float smoothSpeed = 2.5f;

    void Start()
    {
        transform.position = player.transform.position + offset;
    }

    void FixedUpdate()
    {
        Vector3 desiredPosition = player.transform.position + offset;
        Vector3 smoothedPosition = Vector3.Lerp(transform.position, desiredPosition, smoothSpeed * Time.deltaTime);
        transform.position = smoothedPosition;
    }
}