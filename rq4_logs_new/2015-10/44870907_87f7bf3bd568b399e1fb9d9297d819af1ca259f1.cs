using UnityEngine;
using Windows.Kinect;

using System;
using System.Collections;

public class RobotController4 : MonoBehaviour 
{
	public bool MoveVertically = false;
	public bool MirroredMovement = false;
	
	//public GameObject debugText;
	public int PLAYERNUM;

	public GameObject Hip_Center;
	public GameObject Spine;
	public GameObject Neck;
	public GameObject Head;
	public GameObject Shoulder_Left;
	public GameObject Elbow_Left;
	public GameObject Wrist_Left;
	public GameObject Hand_Left;
	public GameObject Shoulder_Right;
	public GameObject Elbow_Right;
	public GameObject Wrist_Right;
	public GameObject Hand_Right;
	public GameObject Hip_Left;
	public GameObject Knee_Left;
	public GameObject Ankle_Left;
	public GameObject Foot_Left;
	public GameObject Hip_Right;
	public GameObject Knee_Right;
	public GameObject Ankle_Right;
	public GameObject Foot_Right;
	public GameObject Spine_Shoulder;
	public GameObject Hand_Tip_Left;
	public GameObject Thumb_Left;
	public GameObject Hand_Tip_Right;
	public GameObject Thumb_Right;
	
	public LineRenderer LinePrefab;
	//	public LineRenderer DebugLine;
	
	private GameObject[] bones;
	private LineRenderer[] lines;
	
	private LineRenderer lineTLeft;
	private LineRenderer lineTRight;
	private LineRenderer lineFLeft;
	private LineRenderer lineFRight;


	public Vector3 hipRotation;
	
	private Vector3 initialPosition;
	private Quaternion initialRotation;
	private Vector3 initialPosOffset = Vector3.zero;
	private Int64 initialPosUserID = 0;

	private Int64 userID = 0;
	
	public Vector3 leftAnklePos;
	public Vector3 rightAnklePos;
	//public bool stepLeft=false;
	//public bool stepRight=false;
	public float stepSensitivity=.5f;
	public bool checkKicks=false;

	public bool leftArmOn = true;
		public bool rightArmOn = true;
	public bool torsoOn = true;
	
	private KinectInterop.HandState leftHandState = KinectInterop.HandState.Unknown;
	private KinectInterop.HandState rightHandState = KinectInterop.HandState.Unknown;

	public float leftHandDist;
	public float rightHandDist;

	
	void Start () 
	{
		//store bones in a list for easier access
		bones = new GameObject[] {
			Hip_Center,
			Spine,
			Neck,
			Head,
			Shoulder_Left,
			Elbow_Left,
			Wrist_Left,
			Hand_Left,
			Shoulder_Right,
			Elbow_Right,
			Wrist_Right,
			Hand_Right,
			Hip_Left,
			Knee_Left,
			Ankle_Left,
			Foot_Left,
			Hip_Right,
			Knee_Right,
			Ankle_Right,
			Foot_Right,
			Spine_Shoulder,
			Hand_Tip_Left,
			Thumb_Left,
			Hand_Tip_Right,
			Thumb_Right
		};
		
	
		initialPosition = transform.position;
		initialRotation = transform.rotation;
		leftAnklePos = new Vector3 (0f, 0f, 2f);
		rightAnklePos = new Vector3 (0f, 0f, 2f);
	}
	
	
	void Update () 
	{
		KinectManager manager = KinectManager.Instance;
		
		// get 1st player
		Int64 userID = manager ? manager.GetPrimaryUserID() : 0;
		/*
		if(userID <= 0)
		{
			// reset the pointman position and rotation
			if(transform.position != initialPosition)
				transform.position = initialPosition;
			
			if(transform.rotation != initialRotation)
				transform.rotation = initialRotation;
			
			for(int i = 0; i < bones.Length; i++) 
			{
				bones[i].gameObject.SetActive(true);
				
				bones[i].transform.localPosition = Vector3.zero;
				bones[i].transform.localRotation = Quaternion.identity;
				
				if(LinePrefab)
				{
					lines[i].gameObject.SetActive(false);
				}
			}
			
			return;
		}
		
		// set the position in space
		Vector3 posPointMan = manager.GetUserPosition(userID);
		posPointMan.z = !MirroredMovement ? -posPointMan.z : posPointMan.z;
		
		// store the initial position
		if(initialPosUserID != userID)
		{
			initialPosUserID = userID;
			initialPosOffset = transform.position - (MoveVertically ? posPointMan : new Vector3(posPointMan.x, 0, posPointMan.z));
		}
		
		transform.position = initialPosOffset + (MoveVertically ? posPointMan : new Vector3(posPointMan.x, 0, posPointMan.z));
		*/
		// update the local positions of the bones
		for(int i = 0; i < bones.Length; i++) 
		{
			if(bones[i] != null)
			{
				int joint = !MirroredMovement ? i : (int)KinectInterop.GetMirrorJoint((KinectInterop.JointType)i);
				//int joint = (int)manager.GetJointAtIndex(
				//	!MirroredMovement ? i : (int)KinectInterop.GetMirrorJoint((KinectInterop.JointType)i));
				if(joint < 0)
					continue;
				
				if(manager.IsJointTracked(userID, joint))
				{
					if(joint==0){
						if(torsoOn){
							bones[i].gameObject.SetActive(true);
						}
					}
						else{
						}
					}
					else if(joint==4){
						if(leftArmOn){
							bones[i].gameObject.SetActive(true);
						}
						else{
						}
					}
					else if(joint==8){
						if(rightArmOn){
							bones[i].gameObject.SetActive(true);
						}
						else{
						}
					}
					else{
						bones[i].gameObject.SetActive(true);
					}
					Vector3 posJoint = manager.GetJointPosition(userID, joint);
					posJoint.z = !MirroredMovement ? -posJoint.z : posJoint.z;
					
					Quaternion rotJoint = manager.GetJointOrientation(userID, joint, !MirroredMovement);

				if(i==0){
					hipRotation= rotJoint.eulerAngles;
					}
					//bones[i].transform.localPosition = posJoint;
					bones[i].transform.localRotation = rotJoint;
					
		
				}
				else
				{
					//bones[i].gameObject.SetActive(false);
				}
			}	
		
		Vector3 leftTip = manager.GetJointPosition (userID, 21);
		Vector3 leftThumb = manager.GetJointPosition (userID, 22);
		Vector3 rightTip = manager.GetJointPosition (userID, 23);
		Vector3 rightThumb = manager.GetJointPosition (userID, 24);
		
		leftHandDist = Vector3.Distance (leftTip, leftThumb);
		rightHandDist = Vector3.Distance (rightTip, rightThumb);

		}

	/*
			if (checkKicks == true) {

			if (Mathf.Abs (leftTemp.z - leftAnklePos.z) >= stepSensitivity) {
				stepAnim.SetBool("stepLeft",true);
			}
			if (Mathf.Abs (rightTemp.z - rightAnklePos.z) >= stepSensitivity) {
				stepAnim.SetBool("stepRight",true);
			}

		}
		
		leftAnklePos=leftTemp;
		rightAnklePos=rightTemp;
		checkKicks=true;
		*/


	void LateUpdate(){
	//	stepAnim.SetBool ("stepLeft", false);
		//stepAnim.SetBool ("stepRight", false);
	}
	
	
}