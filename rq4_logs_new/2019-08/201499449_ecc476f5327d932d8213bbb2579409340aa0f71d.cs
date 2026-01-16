using UnityEngine;

public class CameraMovement : MonoBehaviour
{
    [SerializeField] private float speed = 5;
    [SerializeField] private Vector3 lowerBounds;
    [SerializeField] private Vector3 upperBounds;
    
    private void Update()
    {
        var pos = transform.position;
        if (Input.GetKey(KeyCode.W))
        {
            pos += Time.deltaTime * speed * Vector3.forward;
        }
        if (Input.GetKey(KeyCode.S))
        {
            pos += Time.deltaTime * speed * Vector3.back;
        }
        if (Input.GetKey(KeyCode.A))
        {
            pos += Time.deltaTime * speed * Vector3.left;
        }
        if (Input.GetKey(KeyCode.D))
        {
            pos += Time.deltaTime * speed * Vector3.right;
        }

        pos = Vector3.Max(pos, lowerBounds);
        pos = Vector3.Min(pos, upperBounds);
        transform.position = pos;
    }
}