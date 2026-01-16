using UnityEngine;
using System.Collections;

public class FollowTarget : MonoBehaviour 
{
	[SerializeField] private GameObject _target;
	private Vector3 _offsetTransform;

	void Start () 
	{
		_offsetTransform = this.transform.position - _target.transform.position;
	}

	void Update () 
	{
		this.transform.position = _target.transform.position + _offsetTransform;
	}
}