using System;
using System.Collections;
using UnityEngine;

namespace Overworld {

	public class PlayerFollow : MonoBehaviour {

		public GameObject following;
		public float offSet = 12f;
		private Vector3 camPos;
		private Camera cam;

		void Start () {
			cam = Camera.main;

			if (following == null) {
				following = GameObject.FindGameObjectWithTag(TagConstants.OVERWORLDPLAYER);
			}
			camPos = cam.gameObject.transform.position;
		}

		void Update () {
			cam.gameObject.transform.position = new Vector3(following.gameObject.transform.position.x,camPos.y,following.transform.position.z - offSet);
		}
	}
}