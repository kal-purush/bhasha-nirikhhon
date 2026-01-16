using UnityEngine;

public class MainCamera : MonoBehaviour {
    private GameObject mainCamera;
    private GameObject player;

	// Use this for initialization
	void Start () {
        mainCamera = gameObject;
        player = FindObjectOfType<PlayerCharacter>().gameObject;
	}
	
	// Update is called once per frame
	void Update () {
        Vector3 playerPosition = player.transform.position;
        Vector3 cameraPosition = mainCamera.transform.position;
        Vector3 cameraHeight = new Vector3(0, 0, -10);

        Vector3 movement = playerPosition - cameraPosition + cameraHeight;

        mainCamera.transform.Translate(movement);
	}
}