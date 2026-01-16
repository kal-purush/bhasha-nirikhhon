using UnityEngine;

[ExecuteAlways]
public class LookAt : MonoBehaviour
{
    [SerializeField] Transform target;
    void Update()
    {
        if (target != null)
        {
            Vector3 direction = target.position - transform.position;   // Ir�ny kisz�mol
            direction.y = 0;                                            // F�gg�leges komponnes ki�t
            transform.rotation = Quaternion.LookRotation(direction);
        }
    }
}