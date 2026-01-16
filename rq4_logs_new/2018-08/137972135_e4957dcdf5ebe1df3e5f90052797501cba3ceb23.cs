using UnityEngine;

public abstract class BaseDevice : MonoBehaviour
{

	public float radius = 3.5f;

	private void OnMouseDown()
	{
		Transform player = GameObject.FindWithTag("Player").transform;
		if (Vector3.Distance(player.position, transform.position) < radius)
		{
			if (IsTransformFacing(player))
			{
				Operate();
			}
		}
	}

	private bool IsTransformFacing(Transform player)
	{
		Vector3 direction = transform.position - player.position;
		return Vector3.Dot(player.forward, direction) > 0.5f;
	}

	public virtual void Operate()
	{
		// behavior of specific device
	}
}