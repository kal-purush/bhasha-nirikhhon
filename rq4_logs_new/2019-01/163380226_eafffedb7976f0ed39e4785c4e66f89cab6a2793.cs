using UnityEngine;

public class CameraController : MonoBehaviour
{
    [SerializeField] private GameObject _target;
    [SerializeField] private float _heigth;
    [SerializeField] private float _width;

    void Update()
    {
        Move();
    }

    void Move()
    {        
        Vector3 position = _target.transform.position;
        if (position.x < -_width) position.x = -_width;
        if (position.x > _width) position.x = _width;
        if (position.y < -_heigth) position.y = -_heigth;
        if (position.y > _heigth) position.y = _heigth;
        position.z = -10;
        transform.position = position;
    }
}