using UnityEngine;
using System.Collections;

public class transfrom : MonoBehaviour
{

    public float angle = 30f;
    private Vector3 targetPos;
    private Vector3 target;

    // Use this for initialization
    void Start()
    {
        Transform target = GameObject.Find("Sample").transform;
        targetPos = target.position;
        transform.Rotate(new Vector3(0, Random.Range(0, 0), 0), Space.World);

    }

    // Update is called once per frame
    void Update()
    {

        Mathf.Sin(1);
        Mathf.Cos(1);

        Vector3 pos = Vector3.one;
        float speed = 2.0f;

        pos.y += Mathf.Sin(Time.time * speed) * 2f;
        pos.x += Mathf.Cos(Time.time * speed) * 2f;
        target = pos;
        gameObject.transform.position = pos;



        //Vector3 axis = Vector3.forward;
        //transform.RotateAround(targetPos, axis, angle * Time.deltaTime);

    }
}