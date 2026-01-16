using System.Collections;
using System.Collections;
using UnityEngine;

public class ObjectGenerator : MonoBehaviour {

	public Transform generationPoint;

	public float objectWidth;
	public float minObjectDistance;

	public ObjectPooler objectPools;

	// Use this for initialization
	void Start() {
		this.objectWidth = objectPools.pooledObject.GetComponent<BoxCollider2D>().size.x;
	}

	// Update is called once per frame
	void Update () {
		this.minObjectDistance = Random.Range(1, 10);
		if (this.transform.position.x < generationPoint.position.x)
		{
			transform.position = new Vector3(transform.position.x + this.minObjectDistance, transform.position.y - this.objectWidth + 0.7f, transform.position.z);

			GameObject newObject = objectPools.getPooledObject();
			newObject.transform.position = transform.position;
			newObject.transform.rotation = transform.rotation;
			newObject.SetActive(true);

			transform.position = new Vector3(transform.position.x + this.objectWidth + this.minObjectDistance, transform.position.y + this.objectWidth - 0.7f, transform.position.z);
		}
	}    
}