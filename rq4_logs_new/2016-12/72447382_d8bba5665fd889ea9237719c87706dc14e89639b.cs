using UnityEngine;
using UnityEngine.Networking;

public class MiningBehaviour : NetworkBehaviour
{
	private Rigidbody2D playerRb2d;
	private PlayerMovement pm;

	void Start()
	{
		playerRb2d = gameObject.GetComponent<Rigidbody2D> ();
		pm = gameObject.GetComponent<PlayerMovement> ();
	}

	void Update()
	{
		if (!isLocalPlayer)
			return;
		
		if (Input.GetButtonDown ("Pick axe")) {
			CmdMine ();
		}
	}

	[Command]
	public void CmdMine() {
		Vector2 direction = Vector2.down;

		if (pm.dir == Dir.West) {
			direction = Vector2.left;
		} else if (pm.dir == Dir.North) {
			direction = Vector2.up;
		} else if (pm.dir == Dir.East) {
			direction = Vector2.right;
		} else if (pm.dir == Dir.South) {
			direction = Vector2.down;
		}

		int layermask = 1 << LayerMask.NameToLayer ("Obstacles");
		RaycastHit2D hit = Physics2D.Raycast (playerRb2d.position, direction, 1.1f, layermask);

		if (hit.collider != null) {
			if (hit.collider.gameObject.CompareTag ("Obstacles")) {
				hit.collider.gameObject.GetComponent<ObstacleBehaviour> ().TakeHit ();
			}
		}
	}
}