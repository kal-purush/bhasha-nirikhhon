using UnityEngine;
using System.Collections;

public class CameraController : MonoBehaviour {
	public GameObject gm;
	// Use this for initialization
	private GameObject fallenPlayer;
	private float smooth;
	private float size;
	private GameManager script;

	void Start () {
		smooth = 3.0F;
		size = Camera.main.orthographicSize;
		script = gm.GetComponent<GameManager>();

	}

	// Update is called once per frame
	void FixedUpdate () {
		//Camera.main.orthographicSize = size;
		if (script.isFallen) {
			fallenPlayer = script.fallenPlayer;	
			Camera.main.orthographicSize = 350;
			transform.position = Vector3.Lerp (transform.position, fallenPlayer.transform.position, Time.deltaTime * smooth);
            //check if camera is at right position 
			/*if (Mathf.Abs(transform.position.x - fallenPlayer.transform.position.x)<20 && Mathf.Abs(transform.position.y - fallenPlayer.transform.position.y)<20) {
                script.isFallen = false;
				fallenPlayer.GetComponent<PlayerController> ().triggerPlayerFall ();
			}*/

		} 
	}
}