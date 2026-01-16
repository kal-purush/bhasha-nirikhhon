using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class Arrow : MonoBehaviour
{
    public float speed  = 10f;
    public Transform tip;
    private Rigidbody _rigidBody;
    private bool _inAir = false;
    private Vector3 _lastPosition = Vector3.zero;

    private void Awake()
    {
        _rigidBody = GetComponent<Rigidbody>();
        BowBehaviour.PullActionReleased += Release;
        Stop();
    }

    private void OnDestroy()
    {
        BowBehaviour.PullActionReleased -= Release;
    }
    // Start is called before the first frame update
    private void Release(float value)
    {
        BowBehaviour.PullActionReleased -= Release;
        gameObject.transform.parent = null;
        _inAir = true;
        SetPhysics(true);
        Vector3 force = transform.forward * value * speed;
        _rigidBody.AddForce(force, ForceMode.Impulse);

        StartCoroutine(RotateWithVelocity());
        _lastPosition = tip.position;
    }
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    void FixedUpdate()
    {
        if(_inAir)
        {
            CheckCollision();
            _lastPosition = tip.position;
        }
    }
    private void CheckCollision()
    {
        if(Physics.Linecast(_lastPosition, tip.position, out RaycastHit hitInfo))
        {
            if(hitInfo.transform.gameObject.tag != "ArrowIgnore")
            {
                if(hitInfo.transform.TryGetComponent(out Rigidbody body))
                {
                    _rigidBody.interpolation = RigidbodyInterpolation.None;
                    transform.parent = hitInfo.transform;
                    body.AddForce(_rigidBody.velocity, ForceMode.Impulse);
                }
                Stop();
            }
        }
    }

    private IEnumerator RotateWithVelocity()
    {
        yield return new WaitForFixedUpdate();
        while(_inAir)
        {
            Quaternion newRotation = Quaternion.LookRotation(_rigidBody.velocity, transform.up);
            transform.rotation = newRotation;
            yield return null;
        }
    }
    private void Stop()
    {
        _inAir = false;
        SetPhysics(false);
    }
    private void SetPhysics(bool usePhysics)
    {
        _rigidBody.useGravity = usePhysics;
        _rigidBody.isKinematic = !usePhysics;
    }
}