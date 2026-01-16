using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Vuforia;

public class BoxTracking_bl : MonoBehaviour, ITrackableEventHandler
{

    private TrackableBehaviour mTrackableBehaviour;
    public bool is_detected_ = false;
    public ControlBox bas;
  

    // Use this for initialization
    void Start()
    {
        mTrackableBehaviour = GetComponent<TrackableBehaviour>();
        if (mTrackableBehaviour)
        {
            mTrackableBehaviour.RegisterTrackableEventHandler(this);
        }
    }

    public void OnTrackableStateChanged(TrackableBehaviour.Status previousStatus, TrackableBehaviour.Status newStatus)
    {
        if (newStatus == TrackableBehaviour.Status.DETECTED || newStatus == TrackableBehaviour.Status.TRACKED)
        {

            is_detected_ = true;
            Debug.Log("true");
        }
        else
        {
            is_detected_ = false;
            Debug.Log("false");
        }
    }
}