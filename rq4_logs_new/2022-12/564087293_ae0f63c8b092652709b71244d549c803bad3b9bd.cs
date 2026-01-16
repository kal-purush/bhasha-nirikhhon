using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VRControllerTrack : MonoBehaviour
{
    // Start is called before the first frame update
    public Transform handBone;
    public Transform armBone;
    public Transform headset;


    public int direction;


    bool ready = false;
    void Start()
    {
        StartCoroutine(IsReady());
    }

    // Update is called once per frame
    void Update()
    {
        
        
        if(ready) 
        {
            
            Quaternion x = Quaternion.FromToRotation(-headset.parent.right * direction, (headset.position - this.transform.position).normalized) * Quaternion.AngleAxis(direction * 80, Vector3.forward);
            armBone.localRotation = x;
            
            //Debug.Log(Mathf.Acos(Vector3.Dot(headset.right * -direction, (headset.localPosition - this.transform.localPosition).normalized)) * 180 / Mathf.PI);
            //armBone.localRotation = 
            handBone.localRotation = Quaternion.Inverse(x) * this.transform.localRotation * Quaternion.AngleAxis(-90, Vector3.right) ;
        }
    }
    IEnumerator IsReady()
    {
        yield return new WaitForSeconds(1.0f);
        ready = true;
    }
}