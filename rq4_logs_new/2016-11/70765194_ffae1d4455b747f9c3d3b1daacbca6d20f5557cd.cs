using UnityEngine;
using System.Collections;

public class MoveInko : MonoBehaviour
{

    float moveSpeed;

    float moveTime;


    RectTransform rectTransform = null;

    [SerializeField]
    float moveChangeTime = 1.0f;

    void Start()
    {
        rectTransform = this.gameObject.GetComponent<RectTransform>();
        moveSpeed = 0.05f;
        moveTime = 0.0f;
    }

    void Update()
    {
        moveTime += Time.deltaTime;
        var Rotate = this.GetComponent<RectTransform>().rotation;
        Rotate.z += Time.deltaTime * moveSpeed;
        this.GetComponent<RectTransform>().rotation = Rotate;

        if (moveTime > moveChangeTime)
        {
            moveTime = 0.0f;
            moveSpeed *= -1;
        }
    }
}