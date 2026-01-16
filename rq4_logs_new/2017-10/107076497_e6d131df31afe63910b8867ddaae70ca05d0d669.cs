using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using UnityEngine;

public class Player : MonoBehaviour
{
    [SerializeField]
    float speed = 10.0f;

    [SerializeField]
    private Text text = null;

    void Update()
    {
        var dir = Vector3.zero;
        dir.x = Input.acceleration.x;
        dir.y = Input.acceleration.y;

        text.GetComponent<Text>().text = dir.ToString();

        if (dir.sqrMagnitude > 1)
        {
            dir.Normalize();
        }

        dir *= Time.deltaTime;

        transform.Translate(dir * speed);
    }
}