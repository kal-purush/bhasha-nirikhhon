using UnityEngine;
using System.Collections;

public class ABrotator : MonoBehaviour {

	void Update () {
        transform.Rotate(new Vector3(15, 30, 45) * Time.deltaTime);
	}
}