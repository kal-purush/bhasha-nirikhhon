using UnityEngine;
using System.Collections;

public class Lerping : MonoBehaviour
{

    public Vector3 posA;
    public Vector3 posB;
    public float speed;
    private Transform trans;
    private bool isUp = false;

    void Awake()
    {
        trans = transform;
    }

    void Update()
    {
        if (isUp == false)
        {
            trans.position = Vector3.Lerp(trans.position, posB, Time.deltaTime * speed);
            if (Mathf.Abs(posB.y - trans.position.y) < 0.05)
            {
                trans.position = posB;
                isUp = true;
            }

        }

        else
        {
            trans.position = Vector3.Lerp(trans.position, posA, Time.deltaTime * speed);
            if (Mathf.Abs(posA.y - trans.position.y) < 0.05)
            {
                trans.position = posA;
                isUp = false;
            }
        }
    }
}


   