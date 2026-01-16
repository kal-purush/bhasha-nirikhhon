using UnityEngine;
using System.Collections;

public class CleanUpPhysics : MonoBehaviour {

	public float waitTime = 2f;

	void Start()
	{
		StartCoroutine( "DoCleanUp" );
	}

	private IEnumerator DoCleanUp()
	{
		yield return new WaitForSeconds( waitTime );

		Rigidbody[] rigidbodies = gameObject.GetComponentsInChildren<Rigidbody>();

		for( int i = 0; i < rigidbodies.Length; i++ )
			Destroy( rigidbodies[i] );

		Collider[] colliders = gameObject.GetComponentsInChildren<Collider>();

		for( int i = 0; i < colliders.Length; i++ )
			Destroy( colliders[i] );
	}
}