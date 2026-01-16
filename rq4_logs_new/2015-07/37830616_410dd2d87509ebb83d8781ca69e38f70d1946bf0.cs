using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class RoomUI : MonoBehaviour {

	public GameObject linkPrefab;

	public List<GameObject> buttons = new List<GameObject>();

	private Room room;

	public void Start(){

	}

	public void Update(){
		//TODO Fix this shit. I dont know how to set the canvas pos, when its child of a gameOgbejt with pos != 000
		//transform.FindChild ("ScreenCanvas").GetComponent<Canvas> ().transform.position = new Vector3 (Screen.width/2, Screen.height/2,0);
	}

	public void OnClickToLink(){
		buttons.AddRange(room.NextRoomButtonClick (linkPrefab));
	}

	public void OnClickCancelLinkButtons(){
		foreach (GameObject go in buttons) {
			Destroy(go.transform.parent.gameObject);
		}
		buttons.Clear();
	}

	Room Room {
		get {
			return this.room;
		}
		set {
			room = value;
		}
	}

}