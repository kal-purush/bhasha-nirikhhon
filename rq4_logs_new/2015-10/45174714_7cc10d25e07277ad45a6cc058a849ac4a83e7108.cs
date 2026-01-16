using UnityEngine;
using System.Collections;

public class LevelPieceScript : MonoBehaviour {
	private float speed = 100f;
	[SerializeField]
	private GameObject[] possibleParts;

	// Use this for initialization
	void Start () {
		foreach(Transform child in transform){
			if (child.CompareTag("Part Placeholder")) {
				GameObject nextPartPrefab = possibleParts[Random.Range(0, possibleParts.Length)];
				GameObject part = Instantiate(nextPartPrefab, child.position, Quaternion.identity) as GameObject;
				part.transform.SetParent(transform);

				Destroy(child.gameObject);
			}
		}

		GetComponent<Rigidbody2D> ().AddForce (Vector2.down * speed);
	}
}