using UnityEngine;
using System.Collections;

public class WallControl : MonoBehaviour {

	private SceneGridManager gridManager;
	
	void Start()
	{
		gridManager = GameObject.FindGameObjectWithTag("GridManager").GetComponent<SceneGridManager>();
		BoxCollider2D collider = this.gameObject.GetComponent<BoxCollider2D>();
		gridManager.registerObject(this.gameObject, collider.transform.position.x, collider.transform.position.y, collider.size.x, collider.size.y, 5);
	}
	
	void OnDestroy()
	{
		gridManager.removeObject(this.gameObject);
	}
}