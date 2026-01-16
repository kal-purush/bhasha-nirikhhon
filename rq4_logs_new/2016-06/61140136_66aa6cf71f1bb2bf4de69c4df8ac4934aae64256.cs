using UnityEngine;
using System.Collections;

public abstract class AbstractTargetFollower : MonoBehaviour {
	public enum UpdateType
	{
		FixedUpdate,
		LateUpdate,
		ManualUpdate,
	}
	[SerializeField] protected Transform target;
	[SerializeField] private bool autoTargetPlayer = true;
	[SerializeField] private UpdateType updateType;

	protected Rigidbody targetRigidbody;

	protected virtual void Start()
	{
		// if auto targeting is used, find the object tagged "Player"
		// any class inheriting from this should call base.Start() to perform this action!
		if (this.autoTargetPlayer) {
			FindAndTargetPlayer ();
		}
	}

	private void FixedUpdate()
	{
		// we update from here if updatetype is set to Fixed, or in auto mode,
		// if the target has a rigidbody, and isn't kinematic.
		if (this.autoTargetPlayer && (this.target == null || !this.target.gameObject.activeSelf))
		{
			FindAndTargetPlayer();
		}
		if (this.updateType == UpdateType.FixedUpdate)
		{
			FollowTarget(Time.deltaTime);
		}
	}

	public void LateUpdate()
	{
		if (this.autoTargetPlayer && (this.target == null || !this.target.gameObject.activeSelf)){
			FindAndTargetPlayer ();
		}
		if (this.updateType == UpdateType.LateUpdate) {
			FollowTarget (Time.deltaTime);
		}
	}
	public void ManualUpdate()
	{
		// we update from here if updatetype is set to Late, or in auto mode,
		// if the target does not have a rigidbody, or - does have a rigidbody but is set to kinematic.
		if (this.autoTargetPlayer && (this.target == null || !this.target.gameObject.activeSelf)) {
			FindAndTargetPlayer ();
		}
		if (this.updateType == UpdateType.ManualUpdate) {
			FollowTarget (Time.deltaTime);
		}
	}
	protected abstract void FollowTarget (float deltaTime);

	public void FindAndTargetPlayer()
	{
		var targetObj = GameObject.FindGameObjectWithTag ("Player");
		if (targetObj) {
			SetTarget (targetObj.transform);
		}
	}

	public virtual void SetTarget(Transform newTransform)
	{
		this.target = newTransform;
	}

	public Transform Target
	{
		get { return this.target; }
	}
}