using UnityEngine;
using System.Collections;

public class PortalTrigger : MonoBehaviour {
	public GameObject portalOut; //Object buat Portal Keluar
	void Start() {
	}

	//Fungsi buat trigger portal masuk
	void OnTriggerEnter (Collider other) {
		GameObject objPlayer = other.gameObject;
		if (other.gameObject.tag == "Player") {
			objPlayer.tag="Player";
			objPlayer.transform.position = portalOut.transform.position + new Vector3 (-1.0f, 5.0f, 0.0f);
			//Ubah posisi Sesuai Portal Outnya
			//Ditambah new Vector soalnya biar pas di tengah
			//Nanti di sesuain sm posisi portalnya
		}
	}
}