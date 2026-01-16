using UnityEngine;
using System.Collections;

public class DestroyAfterSeconds : MonoBehaviour {

	public float waitSeconds;

	IEnumerator Start () {
		yield return new WaitForSeconds (waitSeconds);
		Destroy (gameObject);
	}
}