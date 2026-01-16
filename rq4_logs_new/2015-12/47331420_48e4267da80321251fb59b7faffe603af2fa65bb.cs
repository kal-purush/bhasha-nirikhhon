using UnityEngine;
using System.Collections;

public class DragGameObject : MonoBehaviour {
	
	public GameObject this_item;
	public GameObject cursor_hand;

	public bool onandon;
	void Start(){
		onandon = false;
	}
	void OnMouseDown()
	{
		if (onandon == false)
			onandon = true;
		else
			onandon = false;

		StartCoroutine ("move_item");
	}
	IEnumerator move_item(){
		if (onandon) {
			GameObject.Find ("blank").GetComponent<inven> ().dragitem = true ;
			
			Vector3 scrSpace = Camera.main.WorldToScreenPoint (transform.position);
			Vector3 offset = transform.position - Camera.main.ScreenToWorldPoint 
				(new Vector3 (Input.mousePosition.x, Input.mousePosition.y, scrSpace.z));
			
			
			
			while(true){
				Vector3 curScreenSpace = new Vector3 (Input.mousePosition.x, Input.mousePosition.y, scrSpace.z);
				Vector3 curPosition = Camera.main.ScreenToWorldPoint(curScreenSpace) + offset;
				transform.position = curPosition;

				yield return null;
			}
		}

	}
	IEnumerator OnMouseUpAsButton() {
		if (!onandon) {
			Cursor.visible = true;
			cursor_hand.SetActive (false);
		
			yield return new WaitForSeconds (0.001f);
			GameObject.Find ("item").GetComponent <manage_inven> ().pop_item (this_item);
			GameObject.Find ("item").GetComponent <manage_inven> ().push_item (this_item);
			GameObject.Find ("blank").GetComponent<inven> ().dragitem = false;
		}
	}
}