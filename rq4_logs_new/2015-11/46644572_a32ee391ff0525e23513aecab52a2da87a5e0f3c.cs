//Made by Alexandre Jannuzzi & Alberto Menezes for Berklee Game Jam 2015

using UnityEngine;
using System.Collections;

public class DestroyAtBoundary : MonoBehaviour {

	private Rigidbody rb;
	private Rigidbody rb1;

	// Whichever objects leave the boundaries are destroyed (in case the players are thrown off of the board)
	IEnumerator OnTriggerExit(Collider other) {

		Vector3 player1position = new Vector3 (12.0F, 0.5F, 0F);
		Vector3 player2position = new Vector3 (-12.0F, 0.5F, 0F);

		//This spawns the player 1 back to its spawn position
		if (other.gameObject.CompareTag ("Player1")) {
			rb = other.gameObject.GetComponent <Rigidbody> ();
			rb.constraints = RigidbodyConstraints.FreezeAll;
			yield return new WaitForSeconds (1);
			other.gameObject.transform.position = player1position;
			rb.constraints = RigidbodyConstraints.None;
		}

		//This spawns the player 2 back to its spawn position
		if (other.gameObject.CompareTag ("Player2")) {
			rb1 = other.gameObject.GetComponent <Rigidbody> ();
			rb1.constraints = RigidbodyConstraints.FreezeAll;
			yield return new WaitForSeconds (1);
			other.gameObject.transform.position = player2position;
			rb1.constraints = RigidbodyConstraints.None;
		}
	}
}