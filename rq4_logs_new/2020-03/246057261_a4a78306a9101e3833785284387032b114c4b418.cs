using System.Collections;
using UnityEngine;

public class MovementAI : MonoBehaviour {

	public float turnSpeed = 3;
	public float turnDst = 1;
	const float pathUpdateMoveThreshold = 1f;
	const float minPathUpdateTime = 3f;
	public float stoppingDst = 0;
	private Vector3 targetPosition;
	private Entity parent;

	private bool isUpdatePathStarted;

	Path path;

	void Start()
	{
		parent = GetComponent<Entity>();
		isUpdatePathStarted = false;
	}

	public void OnPathFound(Vector3[] waypoints, bool success) {
		if (success) {
			this.path = new Path(waypoints, transform.position, turnDst, stoppingDst);
			StopCoroutine("FollowPath");
			StartCoroutine("FollowPath");
		}
	}

	public void NavigateTo(Vector3 position) {
		targetPosition = position;
		if (!isUpdatePathStarted) {
			isUpdatePathStarted = true;
			StartCoroutine(UpdatePath());
		}
	}

	public void EndNavigation() {
		//If update path haven't end yet, end it.
		if (isUpdatePathStarted) {
			StopAllCoroutines();
			isUpdatePathStarted = false;
			parent.velocity = Vector3.zero;
		}
	}

	IEnumerator UpdatePath() {
		
		if (Time.timeSinceLevelLoad < .3f) {
			yield return new WaitForSeconds(.3f);
		}

		PathRequestManager.RequestPath(new PathRequest(transform.position, targetPosition, OnPathFound));
		float sqrMoveThreshold = pathUpdateMoveThreshold * pathUpdateMoveThreshold;
		Vector3 targetPosOld = targetPosition;

		while(true) {
			yield return new WaitForSeconds(minPathUpdateTime);
			if((targetPosition - targetPosOld).sqrMagnitude > sqrMoveThreshold) {
				PathRequestManager.RequestPath(new PathRequest(transform.position, targetPosition, OnPathFound));
				targetPosOld = targetPosition;
			}
		}
	}
	
	IEnumerator FollowPath() {
		bool followingPath = true;
		int pathIndex = 0;
		//transform.LookAt(path.lookPoints[0]);

		float speedPercent = 1;

		while (followingPath) {
			Vector2 pos2D = new Vector2(transform.position.x, transform.position.z);
			while (path.turnBoundaries[pathIndex].HasCrossedLine(pos2D)) {
				if (pathIndex == path.finishLineIndex) {
					followingPath = false;
					parent.velocity = Vector3.zero;
					break;
				} else {
					pathIndex++;
				}
			}

			if (followingPath) {
				if (pathIndex >= path.slowDownIndex && stoppingDst > 0) {
					speedPercent = path.turnBoundaries[path.finishLineIndex].DistanceFromPoint(pos2D) / stoppingDst;
					if (speedPercent < 0.01f) {
						followingPath = false;
						parent.velocity = Vector3.zero;
					}
				}
				Quaternion targetRotation = Quaternion.LookRotation(path.lookPoints[pathIndex] - transform.position);
				Quaternion rot = Quaternion.Lerp(transform.rotation, targetRotation, Time.deltaTime * turnSpeed);
				parent.direction = Vector3.up * rot.eulerAngles.y;
				parent.velocity = Vector3.forward;
			}

			yield return null;
		}
	}

	
	public void OnDrawGizmos() {
		if (path != null) {
			path.DrawWithGizmos ();
		}
	}
}