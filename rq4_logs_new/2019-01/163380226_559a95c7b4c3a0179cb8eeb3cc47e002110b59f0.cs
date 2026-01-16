using UnityEngine;

public class CameraController : MonoBehaviour
{
    public GameObject target;
    public float Left;
    public float Right;
    public float Up;
    public float Down;

    void Update()
    {
        Move();
    }

    void Move()
    {
        Vector3 temp = transform.position;
        if (target.transform.position.x > Left && target.transform.position.x < Right)
            temp.x = target.transform.position.x;
        if (target.transform.position.y > Down && target.transform.position.y < Up)
            temp.y = target.transform.position.y;
        transform.position = temp;
    }

}