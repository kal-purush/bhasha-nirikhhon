using System;
using UnityEngine;
using UnityStandardAssets.CrossPlatformInput;

namespace UnityStandardAssets._2D
{
	[RequireComponent(typeof (PlatformerCharacter2D))]
	public class Platformer2DUserControl : MonoBehaviour
	{
		private PlatformerCharacter2D m_Character;
		//private bool m_Jump;
		private float hbuff;
		//rudimentary kill floor setup (KF)
		//public Transform respawn;
		//private float killFloor;
		//public int AP = 50;
		//public int APStart = 50;

		void Start () {
		//	killFloor = -25.0f;
			//APStart = 50;
			//AP = 50;
		}
		private void Awake()
		{
			m_Character = GetComponent<PlatformerCharacter2D>();
		}


		private void Update()
		{
			/*if (!m_Jump)
            {
                // Read the jump input in Update so button presses aren't missed.
				if (Input.touchCount > 0)
					m_Jump = true;
            }*/
			//if (!m_Jump)
			//	m_Jump = Input.GetAxis ("Jump") > 0;
			//if (AP <= 0) {
			//print ("out of ap!");
			//	return;
			//}
			//(KF)
			//if (killFloor < (transform.position.y - 25.0f))
			//	killFloor = transform.position.y - 25.0f;
			//if (transform.position.y < killFloor)
			//{
				/*
				this.transform.position = respawn.position; 	//respawn at beginning
				this.transform.rotation = respawn.rotation;
				killFloor = transform.position.y - 25.0f;
				*/
			//	Application.LoadLevel ("Game Over");
			//}
		}


		private void FixedUpdate()
		{
			//if (AP <= 0) {
			//	print ("out of ap!");
			//	m_Character.Move(0, false, false);
			//	return;
			//}
			// Read the inputs.
			//bool crouch = false;//Input.GetKey(KeyCode.LeftControl);
			float h = Input.GetAxis("Horizontal"); // We're not using andriod anymore so fuck this -> Input.acceleration.x; 
			float input = h;
			if (Mathf.Abs(hbuff) > Mathf.Abs(input))
				h = 0;
			//if (h != 0) {
			//	AP -= 1;
			//}
			//print ("fuckyou");
			//print (AP);
			//print(h);
			//print("are u getting called?!?!?" + " " + h + " " + m_Jump);
			// Pass all parameters to the character control script.
			m_Character.Move(h)//, crouch, m_Jump);
			//m_Jump = false;
			hbuff = input;
		}
	}
}