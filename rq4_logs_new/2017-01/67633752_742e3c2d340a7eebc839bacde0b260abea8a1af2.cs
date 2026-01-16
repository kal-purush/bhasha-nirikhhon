using UnityEngine;
using System.Collections;

public class SwipingGraphics : MonoBehaviour {

    public float lineUpdateSpeed = 0.005f;
    public float maxLineDurationInSeconds = 0.5f;
    private LineRenderer lineRenderer;
    private int numOfVerticesOnLine;
    private bool swiping = false;

    void Awake() {
        lineRenderer = GetComponent<LineRenderer>();
    }

    // Update is called once per frame
    void Update() {

#if UNITY_WEBGL || DEBUG

        // If using editor, you want to use a mouse
        if (Input.GetMouseButtonUp(0) && swiping) {
            // Mouse button released
            CancelInvoke("AddPoint");
            lineRenderer.SetVertexCount(0);
            numOfVerticesOnLine = 0;
            swiping = false;
        } else if (Input.GetMouseButtonDown(0)) {
            // Mouse button released
            InvokeRepeating("AddPoint", 0, lineUpdateSpeed);
            swiping = true;
        } else if (Input.GetMouseButton(0)) {
            // Swiping
        } else if (swiping) {
            print("Still swiping but no released");
        }
#else
        // If not using editor, find touches    
        if (Input.touchCount > 0) {
            if (Input.touches[0].phase == TouchPhase.Began) {
                InvokeRepeating("AddPoint", 0, lineUpdateSpeed);
                swiping = true;
            }
        } else if (swiping) {
            CancelInvoke("AddPoint");
            lineRenderer.SetVertexCount(0);
            numOfVerticesOnLine = 0;
            swiping = false;
        }
#endif
    }


    // Used for drawing attack lines while doing criticals
    public void AddPoint() {
        Vector3 inputPoint;

#if UNITY_WEBGL || DEBUG
            if (!Input.GetMouseButton(0))
                return;
            inputPoint = Input.mousePosition;

#else
        if (Input.touchCount == 0)
                return;
            inputPoint = Input.touches[0].position;
#endif
        // Change input point to 
        inputPoint.z = 10;
        inputPoint = Camera.main.ScreenToWorldPoint(inputPoint);
        lineRenderer.SetVertexCount(++numOfVerticesOnLine);
        lineRenderer.SetPosition(numOfVerticesOnLine-1, inputPoint);

        // Check if swipe has taken too long
        if (numOfVerticesOnLine*lineUpdateSpeed > maxLineDurationInSeconds) {
            numOfVerticesOnLine = 0;
            lineRenderer.SetVertexCount(0);
            CancelInvoke("AddPoint");
        }
    }
}