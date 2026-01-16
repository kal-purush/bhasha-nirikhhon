using UnityEngine;
using System.Collections;

public class LineMaker : MonoBehaviour {

    private LineRenderer lineRenderer;
    private float counter;
    private float dist;
    public Transform origin;
    public Transform destination;
    public Transform destination2;
    public float lineDrawSpeed;
    void Start()
    {
         
        counter = 0;
        lineRenderer = GetComponent<LineRenderer>();
        lineRenderer.SetPosition(0, origin.position);
        lineRenderer.SetPosition(1, destination.position);
        lineRenderer.SetPosition(2, destination2.position);
        this.transform.position = origin.position;
        // lineRenderer.SetPosition(2, Vector3.e(10,10,10));
        lineRenderer.SetWidth(0.33f, 0.33f);
        lineRenderer.useWorldSpace = true;
        dist = Vector3.Distance(origin.position, destination.position);

    }
    void Update()
    {

    }
}