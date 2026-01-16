using UnityEngine;
using System.Collections;

public class PlayerControllerScript : MonoBehaviour {

	// PUBLIC FUNCTION
	public float velocity;
	public float jumpForce;

	// REQUIRED ! ! !
	private Rigidbody rgbody;
	private CapsuleCollider capsuleCollider;
	private Animator animator;
	
	// KEY MAP
	private KeyCode leftKey = KeyCode.LeftArrow;
	private KeyCode rightKey = KeyCode.RightArrow;
	private KeyCode jumpKey = KeyCode.Space;
	
	// MISC
	private bool canJump = true;
	
	void Awake () {
		rgbody = GetComponent<Rigidbody>();
		capsuleCollider = GetComponent<CapsuleCollider>();
		animator = GetComponent<Animator>();
		
		if (rgbody == null) throw new MissingReferenceException("Rigidbody tidak ditemukan");
		if (capsuleCollider == null) throw new MissingReferenceException("Capsule Collider tidak ditemukan");
		if (animator == null) throw new MissingReferenceException("Animator tidak ditemukan");
		
		PhysicMaterial material = new PhysicMaterial();
		material.bounciness = 0;
		material.staticFriction = 0;
		material.dynamicFriction = 0;
		
		capsuleCollider.material = material;
	}

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		InputManager();
	}
	
	void InputManager () {
		bool left = Input.GetKey(leftKey);
		bool right = Input.GetKey(rightKey);
		bool jump = Input.GetKeyDown(jumpKey);
		
		Move(left, right);
		Jump(jump);
	}
	
	void Move(bool left, bool right) {
		if (left) {
			// JALAN KE KIRI
			rgbody.velocity = new Vector3(-velocity, rgbody.velocity.y, 0);
		} else if (right) {
			// JALAN KE KANAN
			rgbody.velocity = new Vector3(velocity, rgbody.velocity.y, 0);
		} else {
			// BERHENTI JALAN
			rgbody.velocity = new Vector3(0, rgbody.velocity.y, 0);
		}
	}
	
	void Jump(bool jump) {
		if (!jump && canJump) return;
		rgbody.velocity = new Vector3(rgbody.velocity.x, jumpForce, 0);
	}
	
	void OnCollisionEnter(Collision other) {
		GameObject objectCollide = other.gameObject;
		if (other.gameObject.tag == "box")
			objectCollide.SendMessage("StepOnBox");
	}
	
	void OnCollisionExit(Collision other) {
		GameObject objectCollide = other.gameObject;
		if (other.gameObject.tag == "box")
			objectCollide.SendMessage("OutFromBox");
	}
	
	public void Dead() {
		
	}
}