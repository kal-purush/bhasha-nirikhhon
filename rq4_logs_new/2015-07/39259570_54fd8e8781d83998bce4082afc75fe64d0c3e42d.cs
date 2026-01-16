using UnityEngine;
using System.Collections;

public class CloudCrafter : MonoBehaviour {

	// Fileds shown in the Inspector pane
	public int numClouds = 40;			// Number of clouds to generate
	public GameObject[] cloudPrefabs;	// Holds the prefabs for the clouds
	public Vector3 cloudPosMin;			// Minimum position for each cloud
	public Vector3 cloudPosMax;			// Maximum position for each cloud
	public float cloudScaleMin = 1.0f;	// Minimum scale for each cloud
	public float cloudScaleMax = 5.0f;	// Minimum scale for each cloud
	public float cloudSpeedMult = 0.5f; // Speed modifier for clouds

	// Fields set dynamically
	private GameObject[] cloudInstances;

	void Awake() {
		// Make an array to hold all cloud instances
		cloudInstances = new GameObject[numClouds];

		// Find the anchor parent
		GameObject anchor = GameObject.Find("CloudAnchor");

		// Iterate through each array element and create a cloud
		GameObject cloud;
		for(int i=0; i<cloudInstances.Length; i++) {
			// Choose a prefab between 0 and cloudPrefabs.Length-1
			int prefabNum = Random.Range(0, cloudPrefabs.Length);
			// Make an Instance
			cloud = Instantiate(cloudPrefabs[prefabNum]) as GameObject;
			// Now position the cloud
			Vector3 cPos = Vector3.zero;
			cPos.x = Random.Range(cloudPosMin.x, cloudPosMax.x);
			cPos.y = Random.Range(cloudPosMin.y, cloudPosMax.y);
			// Randomly scale the cloud
			float scaleFactor = Random.value;
			float scaleValue = Mathf.Lerp(cloudScaleMin, cloudScaleMax, scaleFactor);
			// Smaller clouds with smaller scaleFactor should be nearer to the ground
			cPos.y = Mathf.Lerp(cloudPosMin.y, cPos.y, scaleFactor);
			// Smaller clouds should be farther away
			cPos.z = 100 - 90 * scaleFactor;

			// Apply the transforms to the cloud
			cloud.transform.position = cPos;
			cloud.transform.localScale = Vector3.one * scaleValue;
			// Make the cloud a child of the anchor
			cloud.transform.parent = anchor.transform;
			// Add the cloud to the cloudInstances
			cloudInstances[i] = cloud;
		}
	}

	void Update() {
		// Iterate over all generated clouds
		foreach(GameObject cloud in cloudInstances) {
			// Get cloud scale and position
			Vector3 cPos = cloud.transform.position;
			float scaleValue = cloud.transform.localScale.x;

			// Move lager clouds faster
			cPos.x -= scaleValue * Time.deltaTime * cloudSpeedMult;

			// If cloud moved too far left, set it back to max
			if(cPos.x < cloudPosMin.x){
				cPos.x = cloudPosMax.x;
			}
			// Apply the new position to the cloud
			cloud.transform.position = cPos;
		}
	
	}

}