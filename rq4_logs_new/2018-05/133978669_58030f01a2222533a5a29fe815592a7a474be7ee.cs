using System;
using System.Collections.Generic;
using UnityEngine;



public class CameraMove : MonoBehaviour
{
	public List<CameraPositionTarget> CameraPlaces; //List of target objects to follow and cameras positions 
    private float _stayTimer; 
    private float _transitionTimer;
	private bool _isTransitioning = false;
	private int _currentIndex = 0;
	private int _previousIndex = 0;
    private CameraPositionTarget CurrentCamTarget;
    private bool finished = false;



    void Start()
    {
		Debug.Assert(CameraPlaces.Count > 0);	
		transform.position = CameraPlaces[0].position.position; //Goes to both the start and rotation position
        transform.LookAt(CameraPlaces[0].target.position);
		_stayTimer = CameraPlaces[0].Duration; 
        _transitionTimer = CameraPlaces[0].TransitionDuration;
        CurrentCamTarget = CameraPlaces[0];
	}



	void Update () {
		Debug.Assert(CameraPlaces.Count > 0);
        if (finished) return;
        if ( _isTransitioning )
        {
			_transitionTimer -= Time.deltaTime; //Updates the transition timer on the camera
            var curr = getPrevTarget(); //The updated positions/rotations are smoothed for cameras
            var next = getCurrentTarget();
			float t = 1.0f - (_transitionTimer / curr.TransitionDuration);
			transform.position = smoothstepVec3(curr.position.position, next.position.position, t);
            transform.rotation = Quaternion.LookRotation(smoothstepVec3(curr.direction(), next.direction(), t));    
            if (_transitionTimer < 0.0f) {
				_transitionTimer = curr.TransitionDuration;
				_isTransitioning = false;
			}
		}



        else
        {
			_stayTimer -= Time.deltaTime;
			if (_stayTimer < 0.0f) {
				_isTransitioning = true;
				_previousIndex = _currentIndex;
                _currentIndex = (_currentIndex + 1);
                if (_currentIndex >= CameraPlaces.Count)
                {
                    finished = true;
                }
                else
                {
                    CurrentCamTarget = CameraPlaces[_currentIndex];
                    _stayTimer = CurrentCamTarget.Duration;
                }
			}
		}
	}



	CameraPositionTarget getCurrentTarget() //How to transition between 2 points
    {
		return CameraPlaces[_currentIndex];
	}
	CameraPositionTarget getPrevTarget()
    {
		return CameraPlaces[_previousIndex];
	}
	Vector3 smoothstepVec3(Vector3 a, Vector3 b, float t ) {
		t = Mathf.Clamp01(t);
		return new Vector3(Mathf.SmoothStep(a.x, b.x, t), Mathf.SmoothStep(a.y, b.y, t), Mathf.SmoothStep(a.z, b.z, t));
	}
}