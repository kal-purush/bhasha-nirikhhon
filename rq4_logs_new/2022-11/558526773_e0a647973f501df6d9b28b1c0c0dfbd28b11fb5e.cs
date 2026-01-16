using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class message : MonoBehaviour
{
	public Transform fromLifeline;
	public Transform toLifeline;
	
	private LineRenderer lr;

    // Start is called before the first frame update
    void Start()
    {
        lr = GetComponent<LineRenderer>();
    }

    // Update is called once per frame
    void Update()
    {
    	Vector3 coordsFrom = fromLifeline.localPosition;
    	Vector3 coordsTo = toLifeline.localPosition;
    	
    	// text component positioning
    	float x = (coordsTo.x + coordsFrom.x) / 2;
        transform.localPosition = new Vector3(x, transform.localPosition.y, transform.localPosition.z);
        
        // line renderer positioning
      	float length = coordsTo.x - coordsFrom.x;
        lr.SetPosition(0, new Vector3(-length/2, -15, transform.localPosition.z));
        lr.SetPosition(1, new Vector3(length/2, -15, transform.localPosition.z));
    }
}