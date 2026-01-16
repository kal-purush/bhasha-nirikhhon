using UnityEngine;
using System.Collections;
using System.Collections.Generic;
/// <summary>
/// Controlls the Boards movement.
/// Forward movement, Turning, and stationary Rotation
/// Checks if board is grounded so other classes can use this
/// </summary>
public class Controller : MonoBehaviour {

	public WheelCollider backLeftWheel;
	public WheelCollider backRightWheel;
	public WheelCollider frontLeftWheel;
	public WheelCollider frontRightWheel;

	//public Joystick moveJoystick;

	//eventually make all these part of a Player Stats Class
	public float maxSteer = 20f;
	public float maxSpeed = 10f;
	public float maxBrake = 100f;

	//for steering
	private float _steer = 0.0f;
	private float _speed = 0.0f;
	private float _brake = 0.0f;

	//for stationary rotation
	private float _turnAnglePerFixedUpdate = 0.0f;
	private float _angleVel = 0.3f;

	public List<WheelCollider> wheels;

	WheelHit hit;

	//added 5/22/14 for test purposes
	public float steerAmt = 5f;
	public float speedAmt = 5f;

	private bool isControlEnabled = true;
	private bool isControlNormal = true;

	// Use this for initialization
	void Start () 
	{
		rigidbody.centerOfMass = new Vector3(0,0,0);
	
		wheels.Add(backLeftWheel);
		wheels.Add(backRightWheel);
		wheels.Add(frontLeftWheel);
		wheels.Add(frontRightWheel);

		//initialize controlles
		frontLeftWheel.steerAngle = _steer;
		frontRightWheel.steerAngle = _steer;
		backLeftWheel.brakeTorque = _brake * maxBrake;
		backRightWheel.brakeTorque =  _brake * maxBrake;
		
		_turnAnglePerFixedUpdate = _steer * _angleVel;
	}

	public void ResetWheelAngles()
	{
		frontLeftWheel.steerAngle = 0;
		frontRightWheel.steerAngle = 0;
		backLeftWheel.steerAngle = 0;
		backRightWheel.steerAngle = 0;
	}

	public void DisableControles()
	{
		isControlEnabled = false;
	}

	public void EnableControles()
	{
		isControlEnabled = true;
	}

	public void setControlsReversed()
	{
		if(isControlNormal)
		{
			isControlNormal = false;
		}
		else
		{
			isControlNormal = true;
		}
	}

	private void ReverseControls()
	{
		backLeftWheel.steerAngle = _steer;
		backRightWheel.steerAngle = _steer;
		frontLeftWheel.brakeTorque = _brake * maxBrake;
		frontRightWheel.brakeTorque =  _brake * maxBrake;
		_speed = -_speed;
		_brake = -_brake;
		
		_turnAnglePerFixedUpdate = _steer * _angleVel;	
	}
	private void NormalizeControls()
	{
		frontLeftWheel.steerAngle = _steer;
		frontRightWheel.steerAngle = _steer;
		backLeftWheel.brakeTorque = _brake * maxBrake;
		backRightWheel.brakeTorque =  _brake * maxBrake;
		
		_turnAnglePerFixedUpdate = _steer * _angleVel;	
	}

	public void MovementInput()
	{	
		/*
		//FOR JOYSTICK INPUT
		_steer = moveJoystick.position.x * steerAmt;
		_speed = moveJoystick.position.y * speedAmt;
		_brake = -1 * Mathf.Clamp(moveJoystick.position.y, -1, 0);
		*/
		//
		
		//FOR KEYBOARDINPUT		
		if(isControlEnabled)
		{
			_steer = Input.GetAxis("Horizontal") * maxSteer;
			
			_speed = Input.GetAxis("Vertical") * 10;
			
			
			_speed = Mathf.Clamp(Input.GetAxis("Vertical"), 0, 1) * 10;		
			_brake = -1 * Mathf.Clamp(Input.GetAxis("Vertical"), -1, 0)* 1.5f;

		}
		if(isControlNormal)
		{
			NormalizeControls();
		}
		else
		{
			ReverseControls();
		}
	}

	public void HandleMovement()
	{
		//print(rigidbody.velocity);
		//handle stationary rotation
		if(rigidbody.velocity.magnitude <= 1.2)//was .75
		{	
			Quaternion q = Quaternion.AngleAxis(_turnAnglePerFixedUpdate, transform.up) * transform.rotation;//for new model
			rigidbody.MoveRotation(q);
		}
		//handle movement
		if((rigidbody.velocity.magnitude <= maxSpeed) )// && (!turning))
		{
			rigidbody.AddRelativeForce(Vector3.forward * _speed);
			//rigidbody.AddRelativeForce(transform.forward * speed);
		}
	}

	public bool OllieInput()
	{
		return (Input.GetMouseButtonDown(0) || Input.GetKeyDown(KeyCode.R));//left mouse
		//return false;
	}

	public string TrickInput()
	{
		if(Input.GetKey ("1"))//KickFlip
			return "kickflip";
		else if(Input.GetKey("2"))
			return "popshuvit";
		else if(Input.GetKey("3"))
			return "threeshuv";
		else if(Input.GetKey("4"))
			return "varialkickflip";
		else
			return null;
	}

}

/*
	public void MovementInput(bool reverseControls,bool controlsDisabled)
	{	

		//FOR JOYSTICK INPUT
		_steer = moveJoystick.position.x * steerAmt;
		_speed = moveJoystick.position.y * speedAmt;
		_brake = -1 * Mathf.Clamp(moveJoystick.position.y, -1, 0);

		//		
		//FOR KEYBOARDINPUT

		if(!controlsDisabled)
		{
		_steer = Input.GetAxis("Horizontal") * maxSteer;

		_speed = Input.GetAxis("Vertical") * 10;
		
		
		_speed = Mathf.Clamp(Input.GetAxis("Vertical"), 0, 1) * 10;		
		_brake = -1 * Mathf.Clamp(Input.GetAxis("Vertical"), -1, 0)* 1.5f;

		}

		//NOT USED
		//_speed = Mathf.Clamp(moveJoystick.position.y, 0, 1) * speedAmt;
		//_brake = -1 * Mathf.Clamp(moveJoystick.position.y, -1, 0)* 1.5f;	

		if(!reverseControls)
		{
			frontLeftWheel.steerAngle = _steer;
			frontRightWheel.steerAngle = _steer;
			backLeftWheel.brakeTorque = _brake * maxBrake;
			backRightWheel.brakeTorque =  _brake * maxBrake;
			
			_turnAnglePerFixedUpdate = _steer * _angleVel;	
		}
		//instead of doing it like this just monitor the velocity and when it has changed signes
		//reverse the controls
		else if(reverseControls)
		{
			backLeftWheel.steerAngle = _steer;
			backRightWheel.steerAngle = _steer;
			frontLeftWheel.brakeTorque = _brake * maxBrake;
			frontRightWheel.brakeTorque =  _brake * maxBrake;
			_speed = -_speed;
			_brake = -_brake;

			_turnAnglePerFixedUpdate = _steer * _angleVel;	
		}
	}

*/
/*
	public bool FinishOllieInput()
	{
		if(Input.GetMouseButtonDown(1))//right mouse
		{
			return true;
		}
		return false;
	}
	*/

//Quaternion q = Quaternion.AngleAxis(turnAnglePerFixedUpdate, transform.forward) * transform.rotation;//for old model