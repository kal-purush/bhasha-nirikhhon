// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using UnityEngine;
using System.Collections;

[RequireComponent(typeof(Collider))]
public class TeleportLegacyUI : MonoBehaviour {
	private CardboardHead head;
	private bool mPreviousChoosen;
	private GameObject mainCamera;

  void Awake() {
  	head = Camera.main.GetComponent<StereoController>().Head;
	mainCamera = GameObject.Find("CardboardMain");
//    CardboardOnGUI.IsGUIVisible = true;
//    CardboardOnGUI.onGUICallback += this.OnGUI;
  }

  void Update() {
    RaycastHit hit;
		Collider collider = GetComponent<Collider> ();
		if (collider && head) {
			bool isLookedAt = GetComponent<Collider>().Raycast(head.Gaze, out hit, Mathf.Infinity);
				if (VRTarget.instance != null) {
					if (isLookedAt) {
						mPreviousChoosen = true;
						VRTarget.instance.TargetChosen(isLookedAt);
					} else if (mPreviousChoosen) {
						mPreviousChoosen = false;
						VRTarget.instance.TargetChosen(isLookedAt);
					}
				}
			if (Cardboard.SDK.CardboardTriggered && isLookedAt) {
				//Move to target
				CameraMove.newPosition = this.transform.position;
				if (this.transform.childCount > 0) {
					Transform child = transform.GetChild(0);
					CameraMove.newPosition += Vector3.up*(child.GetComponent<MeshFilter>().mesh.bounds.size.z*child.transform.localScale.z/2f + 100f); //Upward adjustment
					Debug.Log(this.name);
					Debug.Log(child.GetComponent<MeshFilter>().mesh.bounds.size.z);
				} else {
					CameraMove.newPosition += Vector3.up*(GetComponent<MeshFilter>().mesh.bounds.size.z*transform.localScale.z/2f + 100f); //Upward adjustment
					Debug.Log(this.name);
					Debug.Log(GetComponent<MeshFilter>().mesh.bounds.size.z);
				}
			}
		}
//    bool isLookedAt = GetComponent<Collider>().Raycast(head.Gaze, out hit, Mathf.Infinity);
//    SetGazedAt(isLookedAt);
//    if (Cardboard.SDK.CardboardTriggered && isLookedAt) {
//      TeleportRandomly();
//    }
  }

//  void OnGUI() {
////    if (!CardboardOnGUI.OKToDraw(this)) {
////      return;
////    }
////    if (GUI.Button(new Rect(50, 50, 200, 50), "Reset")) {
////      Reset();
////    }
////    if (GUI.Button(new Rect(50, 110, 200, 50), "Recenter")) {
////      Cardboard.SDK.Recenter();
////    }
////    if (GUI.Button(new Rect(50, 170, 200, 50), "VR Mode")) {
////      Cardboard.SDK.ToggleVRMode();
////    }
//  }

//  void OnDestroy() {
//    CardboardOnGUI.onGUICallback -= this.OnGUI;
//  }
}