using UnityEngine;
using System.Collections;

public class FollowPlayer : MonoBehaviour {

    public float interpVelocity;
    public float minDistance;
    public float followDistance;
    private GameObject target;
    public Vector3 offset;
    Vector3 targetPos;
    // Use this for initialization
    void Start()
    {
        target = GameObject.FindGameObjectWithTag("Player");
        targetPos = transform.position;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        if (target)
        {
            Vector3 posNoZ = transform.position;
            posNoZ.z = target.transform.position.z;

            Vector3 targetDirection = (target.transform.position - posNoZ);

            interpVelocity = targetDirection.magnitude * 5f;

            targetPos = transform.position + (targetDirection.normalized * interpVelocity * Time.deltaTime);
            targetPos = new Vector3(targetPos.x, 0, targetPos.z);
            transform.position = Vector3.Lerp(transform.position, targetPos + offset, 0.25f);
        }
    }

}