using UnityEngine;
using UnityEngine.InputSystem;

public class Vacuum : MonoBehaviour
{
    [SerializeField] private float SuctionRadius; // the radius of the suction area
    [SerializeField] private LayerMask SuckableLayers; // the layers that the vacuum can suck

    [SerializeField] private float SuctionStartSpeed;
    [SerializeField] private float SuctionMaxSpeed;
    [SerializeField] private AnimationCurve VelocityCurve;
    [SerializeField] private float AccelerationSpeed;

    private float Accelaration;
    private float Speed;
    private bool IsSucking;

    public void OnSuck(InputAction.CallbackContext context)
    {
        if (!context.started) return;

        IsSucking = !IsSucking;   
    }

    private void Update()
    {
        Suck();
    }
    private void Suck()
    {
        Collider[] colliders = Physics.OverlapSphere(transform.position, SuctionRadius, SuckableLayers);
        foreach (Collider collider in colliders)
        {
            float distance = Vector3.Distance(transform.position, collider.transform.position);
            Vector3 direction = transform.position - collider.transform.position;
            Accelaration += Time.deltaTime * AccelerationSpeed;
            Speed = Mathf.Lerp(SuctionStartSpeed, SuctionMaxSpeed, VelocityCurve.Evaluate(Accelaration / 1f));
            Rigidbody rb = collider.GetComponent<Rigidbody>();
            if (IsSucking)
            {
                if (distance < SuctionRadius)
                {
                    rb.velocity = direction.normalized * Speed;

                    if (distance - SuctionRadius >= 0.1f)
                    {
                        rb.velocity = Vector3.zero;
                        Accelaration = 0;
                    }
                }
            }
            else
            {
                rb.velocity = Vector3.zero;
                Accelaration = 0;
            }
        }
    }
}