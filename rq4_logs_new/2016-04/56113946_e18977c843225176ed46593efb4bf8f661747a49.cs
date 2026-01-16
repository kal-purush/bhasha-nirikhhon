using UnityEngine;
using System.Collections;

public class CameraController : MonoBehaviour
{
    public float moveSensivityX = 10.0f;
    public float moveSensivityY = 10.0f;
    public float orthoZoomSpeed = 0.05f;
    public float minZoom = 1.0f;
    public float maxZoom = 2.0f;
    public bool needUpdateMoveSensivity = true;

    private Camera camera_;
    private float minCameraSize_, maxCameraSize_;
    
    // Use this for initialization
    void Start()
    {
        camera_ = Camera.main;
        // Camera size has invert ratio to zoom level
        maxCameraSize_ = camera_.orthographicSize / minZoom;
        minCameraSize_ = camera_.orthographicSize / maxZoom;
    }

    // Update is called once per frame
    void Update()
    {
        //if (needUpdateMoveSensivity)
        //{
        //    moveSensivityX = camera_.orthographicSize / 5.0f;
        //    moveSensivityY = moveSensivityX;
        //    needUpdateMoveSensivity = false;
        //}

        Touch[] touches = Input.touches;
        // Single touch (move)
        if (touches.Length == 1 && touches[0].phase == TouchPhase.Moved)
        {
            Vector2 delta = touches[0].deltaPosition;
            // Camera moves in opposite direction to touches
            float positionX = - (delta.x * moveSensivityX * Time.deltaTime);
            float positionY = - (delta.y * moveSensivityY * Time.deltaTime);

            camera_.transform.position += new Vector3(positionX, positionY, 0);
        } 
        else if (touches.Length == 2)
        {
            Touch touchOne = touches[0];
            Touch touchTwo = touches[1];

            Vector2 touchOnePrevPos = touchOne.position - touchOne.deltaPosition;
            Vector2 touchTwoPrevPos = touchTwo.position - touchTwo.deltaPosition;

            float prevTouchDistance = (touchOnePrevPos - touchTwoPrevPos).magnitude;
            float currentTouchDistance = (touchOne.position - touchTwo.position).magnitude;
            float distanceDelta = currentTouchDistance - prevTouchDistance;

            float newSize = camera_.orthographicSize - distanceDelta * orthoZoomSpeed;
            if (newSize >= minCameraSize_ && newSize <= maxCameraSize_)
            {
                camera_.orthographicSize = newSize;          
                needUpdateMoveSensivity = true;
            }
        }
    }
}