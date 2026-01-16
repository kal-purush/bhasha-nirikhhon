using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class raycas : MonoBehaviour {
	Camera cam;
	public Text textUI;
	public Text textUI2;
	public Transform player;
	public Transform cup;
	public GameObject mhm;

	// Use this for initialization
	void Start () {
		cam = GetComponent<Camera>();
	}
	
	// Update is called once per frame
	void Update () {

			Ray ray = cam.ScreenPointToRay(new Vector3(400, 400, 0));
		Debug.DrawRay(ray.origin, ray.direction * 100, Color.yellow);
		RaycastHit RayHitInfo = new RaycastHit();
		if ( Physics.Raycast (ray, out RayHitInfo, 100f ) ) {
			if (RayHitInfo.collider.tag == "cup"){
				Debug.Log ("CUP");
			
				mhm.SetActive(true);
				if (!GameManager.instance.zoom){
					textUI.text = "(T) CUP";
					textUI2.text = "(T) CUP";
				
				if (Input.GetKeyDown (KeyCode.T)){
					Camera.main.fieldOfView = 40;
					GameManager.instance.zoom= true;
					textUI.text = "? TOPPLE \n ? DRINK";
					textUI2.text = "? TOPPLE \n ? DRINK";
					}}

		}
		
		}
	}

}