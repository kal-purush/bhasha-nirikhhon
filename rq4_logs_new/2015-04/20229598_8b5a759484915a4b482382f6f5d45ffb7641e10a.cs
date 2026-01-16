using UnityEngine;
using System.Collections.Generic;

public class CSSample : MonoBehaviour {

	void OnMouseDown() {
		Tealium.TrackEvent("CSSample:click", new Dictionary<string, string>(1) {
			{ "lang", "C#" }
		});
	}
}