using UnityEngine;

[RequireComponent(typeof(Rigidbody))]
public class SoulWispEntity : MonoBehaviour
{
    [SerializeField] private Vector2 forceRange;
    private Rigidbody rb;

    private void Awake()
    {
        rb = GetComponent<Rigidbody>();
    }

    private void Start()
    {
        ResetWisp();
    }

    private void Update()
    {
        if (transform.localPosition.y < -3 || transform.localPosition.y > 50) ResetWisp();
    }

    private void ResetWisp()
    {
        transform.localPosition = Vector3.zero;
        rb.AddForce(Random.insideUnitSphere * Random.Range(forceRange.x, forceRange.y), ForceMode.Impulse);
    }
}