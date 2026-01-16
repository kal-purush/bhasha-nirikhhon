using UnityEngine;
using System.Collections;

public class BlockGenerator : MonoBehaviour {
	public bool isCursorLocked = true;
	public Camera playerCmera;
	public float reachableDistance = 5.0f;
	public GameObject blockPrefab;
	public GameObject blockGray;
	float blockWidth;

	Ray ray;
	RaycastHit hitInfo;
	Renderer blockGrayRenderer;
	Vector3 hitObjPos;

	// Use this for initialization
	void Start () {
		if (isCursorLocked) {
			Cursor.visible = false;
		}
		blockWidth = blockPrefab.transform.localScale.x;
		blockGrayRenderer = blockGray.GetComponent<Renderer>();

	}

	// Update is called once per frame
	void Update () {
		if (isCursorLocked) {
			Cursor.lockState = CursorLockMode.Locked;
		}

		ray = playerCmera.ViewportPointToRay (new Vector3 (0.5f, 0.5f, 0.0f));
		bool israyHit = Physics.Raycast (ray, out hitInfo, reachableDistance);

		// Show gray block
		if (israyHit) {
			blockGrayRenderer.enabled = true;
			hitObjPos = hitInfo.collider.gameObject.transform.position;
			blockGray.transform.position = hitObjPos + hitInfo.normal * blockWidth;
		} else {
			blockGrayRenderer.enabled = false;
		}

		// Put blocks
		if (israyHit && Input.GetMouseButtonDown (0)) {
			hitObjPos = hitInfo.collider.gameObject.transform.position;
			Instantiate (blockPrefab, hitObjPos + hitInfo.normal * blockWidth, Quaternion.identity);
		}
		// Destoroy blocks
		if (israyHit && Input.GetMouseButtonDown (1)) {
			Destroy (hitInfo.transform.gameObject);
		}
		// Select blocks

	}
}