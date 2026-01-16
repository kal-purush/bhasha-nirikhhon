using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class ZombieController : MonoBehaviour {

	// Declare Variables

	private Animator anim;
	public GameObject player;
	private Transform target;
	private NavMeshAgent agent;

	public float walkSpeed = 10f;
	public float runSpeed = 5f;
	public float damageRange = 2f;

	private AudioSource growl;

	float touchedTime = 0;

	string currentState = "idle";
	bool attacking = false;

	Vector3 goTo;
	bool inSight;
	bool alive = true;

	private Vector3 startPos;

	// Make a easy function to change the zombies state

	public void SetState(string state){
		if (currentState != state) {
			currentState = state;
			if (state == "idle") {
				agent.speed = 0;
			} else if (state == "running" && runSpeed > 1) {
				agent.speed = runSpeed;
			} else if (state == "walking" || (state == "running" && runSpeed < 1)) {
				agent.speed = walkSpeed;
				currentState = "walking";
			}
			if (attacking == false) {
				anim.SetTrigger ("is" + currentState.Substring (0, 1).ToUpper () + currentState.Substring (1, currentState.Length-1));
			}
		}
	}

	// When the game begings load these components and set variables

	void Start(){
		goTo = transform.position;
		growl = gameObject.GetComponent<AudioSource> ();
		growl.Play ();
		startPos = transform.position;
		target = player.transform;
		anim = GetComponent<Animator> ();
		agent = GetComponent<NavMeshAgent> ();
	}

	// Attack the player when this is called

	private IEnumerator Attack(){
		anim = GetComponent<Animator> ();
		bool killed = false;
		yield return new WaitForSeconds (.1f);
		if ((target.position - transform.position).magnitude < damageRange) { // Double check the range before attacking
			Cutscene cutscene = player.GetComponent<Cutscene>();
			cutscene.Attacked ();
			currentState = "idle";
			killed = true;
			alive = false;
			agent.speed = 0;
		}
	
		yield return new WaitForSeconds (2.03f);
		if (killed) {
			anim.SetTrigger ("isIdle");
			SetState ("idle");
		}else{
			anim.SetTrigger ("is" + currentState.Substring (0, 1).ToUpper () + currentState.Substring (1, currentState.Length-1)); // If it didnt kill return to old anim
		}

		attacking = false;
		CapsuleCollider capCollider = GetComponent<CapsuleCollider> ();
		float rad = capCollider.radius;
		capCollider.isTrigger = false;
		capCollider.radius = 0;
		yield return new WaitForSeconds (.5f);
		capCollider.radius = rad;
		capCollider.isTrigger = true;
	}

	// When theres a collision run this

	void OnTriggerEnter(Collider c){
		if (c.gameObject.CompareTag("Player") && Time.time > touchedTime && alive){ // Check tag to see if it can attack
			attacking = true;
			transform.LookAt (target);
			touchedTime = Time.time + 2.63f;

			anim.SetTrigger ("isAttacking");
			StartCoroutine(Attack ());
		}
	}

	// Every frame do this

	void FixedUpdate(){
		RaycastHit hit;
		int layerMask = 1 << LayerMask.NameToLayer("Monster");
		if (Physics.Linecast(transform.position, player.transform.position, out hit, ~layerMask)){
			print (hit.transform);
			if (hit.transform == player.transform && alive) {
				goTo = player.transform.position;
				inSight = true;
			}else if(inSight && hit.transform != player.transform)  {
				inSight = false;
				goTo = player.transform.position;

			}
			if (currentState == "idle" && inSight) {
				agent.SetDestination (goTo);
			}
		}
	}

	void Update(){
		if (currentState != "idle") {
			agent.SetDestination (goTo);// Move to player if the state is not idle
		}
		if ((agent.remainingDistance  <= agent.stoppingDistance && inSight == false) || alive == false) {
			SetState ("idle");
		}else{
			SetState ("running");
		}

			
	}

}