using UnityEngine;
using System.Collections;

public class get_soulstone : MonoBehaviour {

	public GameObject main_camera;
	IEnumerator OnMouseUpAsButton(){
		GameObject.Find ("soulstone_manager").transform.position = new Vector3 (main_camera.transform.position.x, main_camera.transform.position.y, 0f);
		GameObject.Find ("soulstone_manager").GetComponent<soulstone_manage> ().get_soulstone (1, 2);
		yield return new WaitForSeconds(3f);
		GameObject.Find ("World_map").GetComponent<worldmap_manage> ().go_to ();

	}
}