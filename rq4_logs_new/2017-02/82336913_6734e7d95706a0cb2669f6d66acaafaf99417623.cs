using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PP_Point : MonoBehaviour {
	[SerializeField] Transform mySpriteTransform;
	[SerializeField] SpriteRenderer mySpriteRenderer;
	[SerializeField] SpriteRenderer myBackSpriteRenderer;
	[SerializeField] Color[] myDoneColors; 
	[SerializeField] Color[] myProcessColors; 
	[SerializeField] float myPlayerPower = 1;
	[SerializeField] float myButtPower = 2;
	private float myInvadeLevel = 0; // 100
	private int myInvaderNumber = -1;
	private int myOwnerNumber = -1;
	[SerializeField] float myInvadeLevelMax = 10;

	//	void Start () {
	//	}

	void Update () {
		UpdateColor ();
	}

	void OnTriggerStay2D (Collider2D g_collider2D) {
		float t_ratio = 0;
		int t_teamNumber = -1;
		if (g_collider2D.tag == "Player") {
			t_ratio = myPlayerPower;
			PP_Player t_player = g_collider2D.gameObject.GetComponent<PP_Player> ();
			t_teamNumber = t_player.GetMyTeamNumber ();
		} else if (g_collider2D.tag == "Butt") {
			t_ratio = myButtPower;
			PP_Butt t_butt = g_collider2D.gameObject.GetComponent<PP_Butt> ();
			t_teamNumber = t_butt.GetMyTeamNumber ();
		}

		if (t_teamNumber == -1)
			return;
		
		if (myOwnerNumber == -1) {
			if (myInvaderNumber == -1) {
				myInvaderNumber = t_teamNumber;
				myInvadeLevel += Time.deltaTime * t_ratio;
			} else if (myInvaderNumber == t_teamNumber) {
				myInvadeLevel += Time.deltaTime * t_ratio;
			} else {
				myInvadeLevel -= Time.deltaTime * t_ratio;
				if (myInvadeLevel < 0) {
					myInvadeLevel = -myInvadeLevel;
					myInvaderNumber = t_teamNumber;
				}
			}
		} else {
			myInvaderNumber = 1 - myOwnerNumber;
			if (t_teamNumber == myOwnerNumber) {
				myInvadeLevel -= Time.deltaTime * t_ratio;
			} else {
				myInvadeLevel += Time.deltaTime * t_ratio;
			}
		}

		if (myInvadeLevel >= myInvadeLevelMax) {
			//switch owner
			myOwnerNumber = myInvaderNumber;
			myInvadeLevel = 0;
		} else if (myInvadeLevel < 0) {
			myInvadeLevel = 0;
		}
		//		Debug.Log (myOwnershipLevel);
	}

	private void UpdateColor () {
		if (myOwnerNumber != -1) {
			myBackSpriteRenderer.color = myDoneColors [myOwnerNumber];
		}

		if (myInvaderNumber != -1) {
			mySpriteRenderer.color = myProcessColors [myInvaderNumber];
			mySpriteTransform.localScale = Vector3.one * myInvadeLevel / myInvadeLevelMax;
		}

	}
}