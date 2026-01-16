using UnityEngine;

[RequireComponent (typeof (NavMeshAgent))]
[RequireComponent (typeof (Animator))]
public class LocomotionSimpleAgent : MonoBehaviour {
	Animator _animator;
	NavMeshAgent agent;
	Vector2 smoothDeltaPosition = Vector2.zero;
	Vector2 velocity = Vector2.zero;

	void Start ()
	{
        _animator = GetComponent<Animator> ();
		agent = GetComponent<NavMeshAgent> ();
		agent.updatePosition = false;
	}
	
	void Update ()
	{
		Vector3 worldDeltaPosition = agent.nextPosition - transform.position;

		// Map 'worldDeltaPosition' to local space
		float dx = Vector3.Dot (transform.right, worldDeltaPosition);
		float dy = Vector3.Dot (transform.forward, worldDeltaPosition);
		Vector2 deltaPosition = new Vector2 (dx, dy);

		// Low-pass filter the deltaMove
		float smooth = Mathf.Min(1.0f, Time.deltaTime/0.15f);
		smoothDeltaPosition = Vector2.Lerp (smoothDeltaPosition, deltaPosition, smooth);

		// Update velocity if delta time is safe
		if (Time.deltaTime > 1e-5f)
			velocity = smoothDeltaPosition / Time.deltaTime;

	    if (velocity.magnitude > 0.5f && agent.remainingDistance > agent.radius)
	    {

	        var move = agent.desiredVelocity;

	        if (move.magnitude > 1f)
	        {
	            move.Normalize();
	        }
	        move = transform.InverseTransformDirection(move);
	        move = Vector3.ProjectOnPlane(move, Vector3.up);
	        var turnAmount = Mathf.Atan2(move.x, move.z);
	        var forwardAmount = move.z;



	        // Update animation parameters
	        _animator.SetFloat("Forward", forwardAmount, 0.1f, Time.deltaTime);
	        _animator.SetFloat("Turn", turnAmount, 0.1f, Time.deltaTime);
	    }
	}

	void OnAnimatorMove ()
	{
		// Update postion to agent position
		transform.position = agent.nextPosition;

//		// Update position based on animation movement using navigation surface height
//		Vector3 position = anim.rootPosition;
//		position.y = agent.nextPosition.y;
//		transform.position = position;
	}
}