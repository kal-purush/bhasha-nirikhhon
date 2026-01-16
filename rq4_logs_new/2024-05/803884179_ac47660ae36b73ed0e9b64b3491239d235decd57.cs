using UnityEngine;

public class Moving : MonoBehaviour
{
    private Vector3 _moveDirection = Vector3.forward;

    private void Update()
    {
        Move(_moveDirection);
    }

    private void Move(Vector3 moveDirection)
    {
        transform.Translate(moveDirection * Time.deltaTime);
    }
}