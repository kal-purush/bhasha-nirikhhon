using System;
using UnityEngine;

namespace UnityStandardAssets._2D
{
	public class Camera2DFollow : MonoBehaviour
	{
		//variable to hold the distance between objects that can switch
		//control to
		[SerializeField]
		public float distBetween = 10f;

		//variable to keep track of which object the player is controlling
		public String inControl = "";
		public Transform target;
		public float damping = 1;
		public float lookAheadFactor = 3;
		public float lookAheadReturnSpeed = 0.5f;
		public float lookAheadMoveThreshold = 0.1f;

		private float m_OffsetZ;
		private Vector3 m_LastTargetPosition;
		private Vector3 m_CurrentVelocity;
		private Vector3 m_LookAheadPos;

		//function to toogle script on or off, 0 == off, 1 == on
		private void toogleScript(String targetobj, int OnOff)
		{
			GameObject focus = GameObject.Find(targetobj);
			if (OnOff == 1) 
			{
				focus.GetComponent<Player>().enabled=true;
			} else
			{
				focus.GetComponent<Player>().enabled=false;
			}
		}

		//function to calculate distance between objects
		private float calcDistance(String tmp1, String tmp2){
			Transform tr1 = GameObject.Find(tmp1).transform;
			Transform tr2 = GameObject.Find (tmp2).transform;
			float distance = Vector3.Distance(tr1.position, tr2.position);
			return distance; 
		}

		// Use this for initialization
		private void Start()
		{
			m_LastTargetPosition = target.position;
			m_OffsetZ = (transform.position - target.position).z;
			transform.parent = null;

			inControl = "head";
			toogleScript("Arm (1)", 0);
			toogleScript("Torso", 0);
			toogleScript("Arm", 0);
			toogleScript("Leg", 0);
			toogleScript("Leg (1)", 0);
		}

	

		// Update is called once per frame
		private void Update()
		{
			// Keybindings to number pad to switch Main Camera 
			// to certain game objects
			if (Input.GetKeyUp(KeyCode.Q) && inControl != "head")
			{
				if(calcDistance("Player", inControl) <= distBetween){
					target = GameObject.Find("Player").transform;
					toogleScript(inControl, 0);
					toogleScript("Player", 1);
					inControl = "head";
					Debug.Log(inControl);
				}
			}
			if (Input.GetKeyUp(KeyCode.E))
			{
				if(calcDistance("Player", "Arm (1)") <= distBetween){
					target = GameObject.Find("Arm (1)").transform;

					inControl = "Arm (1)";
					toogleScript("Player", 0);
					toogleScript("Arm (1)", 1);
					Debug.Log(inControl);
				}
			}
			if (Input.GetKeyUp(KeyCode.R))
			{
				if(calcDistance("Player", "Arm") <= distBetween){
					target = GameObject.Find("Arm").transform;
					toogleScript("Player", 0);
					toogleScript("Arm", 1);
					inControl = "Arm";
					Debug.Log(inControl);
				}
			}
			if (Input.GetKeyUp(KeyCode.T))
			{
				if(calcDistance("Player", "Torso") <= distBetween){
					target = GameObject.Find("Torso").transform;
					toogleScript("Player", 0);
					toogleScript("Torso", 1);
					inControl = "Torso";
					Debug.Log(inControl);
				}
			}
			if (Input.GetKeyUp(KeyCode.Y))
			{
				if(calcDistance("Player", "Leg (1)") <= distBetween){
					target = GameObject.Find("Leg (1)").transform;
					toogleScript("Player", 0);
					toogleScript("Leg (1)", 1);
					inControl = "Leg (1)";
					Debug.Log(inControl);
				}
			}
			if (Input.GetKeyUp(KeyCode.U))
			{
				if(calcDistance("Player", "Leg") <= distBetween){
					target = GameObject.Find("Leg").transform;
					toogleScript("Player", 0);
					toogleScript("Leg", 1);
					inControl = "Leg";
					Debug.Log(inControl);
				}
			}
			// only update lookahead pos if accelerating or changed direction
			float xMoveDelta = (target.position - m_LastTargetPosition).x;

			bool updateLookAheadTarget = Mathf.Abs(xMoveDelta) > lookAheadMoveThreshold;

			if (updateLookAheadTarget)
			{
				m_LookAheadPos = lookAheadFactor * Vector3.right * Mathf.Sign(xMoveDelta);
			}
			else
			{
				m_LookAheadPos = Vector3.MoveTowards(m_LookAheadPos, Vector3.zero, Time.deltaTime * lookAheadReturnSpeed);
			}

			Vector3 aheadTargetPos = target.position + m_LookAheadPos + Vector3.forward * m_OffsetZ;
			Vector3 newPos = Vector3.SmoothDamp(transform.position, aheadTargetPos, ref m_CurrentVelocity, damping);

			transform.position = newPos;

			m_LastTargetPosition = target.position;
		}
	}
}