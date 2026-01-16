using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class magicTurretScript : MonoBehaviour
{

    public Transform SpawnTransform;
    public Transform TargetTransform;

    public float AngelInDegrees;

    float g = Physics.gravity.y;

    public GameObject BulletShard;

    void Start()
    {
        
    }


    void Update()
    {
        SpawnTransform.localEulerAngles = new Vector3(-AngelInDegrees, 0f, 0f);
        
        if (Input.GetMouseButtonDown(0))
        {
            Shot();
        }
    }

    public void Shot()
    {
        Vector3 fromTo = TargetTransform.position - transform.position;
        Vector3 fromToXZ = new Vector3(fromTo.x, 0f, fromTo.z);

        transform.rotation = Quaternion.LookRotation(fromToXZ, Vector3.up);

        float x = fromToXZ.magnitude;
        float y = fromTo.y;

        float AngleInRadians = AngelInDegrees * Mathf.PI / 180;

        float v2 = (g * x * x) / (2 * (y - Mathf.Tan(AngleInRadians) * x) * Mathf.Pow(Mathf.Cos(AngleInRadians), 2));
        float v = Mathf.Sqrt(Mathf.Abs(v2));

        GameObject NewBulletShard = Instantiate(BulletShard, SpawnTransform.position, Quaternion.identity);
        NewBulletShard.GetComponent<Rigidbody>().velocity = SpawnTransform.forward * v;
    }

}