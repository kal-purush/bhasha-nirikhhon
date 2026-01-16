using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AlienUI : MonoBehaviour
{
    [SerializeField] private Vector3 direction;
    [SerializeField] private float speed;
    [SerializeField] private Canvas canvas;
    private float timer = 0f;
    private float timerCheck = 0.2f;
    private Vector3 leftEdge;
    private Vector3 rightEdge;
    private Vector3 initialPosition;
    private RectTransform rectTransform;
    private Vector3[] canvasCorners = new Vector3[4];
    private Vector3[] corners = new Vector3[4];
    // Start is called before the first frame update
    void Start()
    {
        rectTransform = GetComponent<RectTransform>();
        initialPosition = transform.position;
        canvas.GetComponent<RectTransform>().GetWorldCorners(canvasCorners);
    }

    // Update is called once per frame
    void Update()
    {
        timer += Time.deltaTime;
        if (timer >= timerCheck)
        {
            transform.Translate(speed * direction);
            rectTransform.GetWorldCorners(corners);
            if (direction == Vector3.right && corners[0].x + speed >= canvasCorners[0].x)
            {
                transform.position = initialPosition;
            }
            else if (direction == Vector3.left && corners[2].x - speed <= canvasCorners[2].x)
            {
                transform.position = initialPosition;
            }

            timer = 0f;
        }
    }
}