using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System.Text;

public class Datapoint : MonoBehaviour {
	
	private static Color color;
	private static Material material;

	void Awake() {
		material = GetComponent<Renderer>().material;

	}
	
	public static void setDataPointColor() {
		if (material != null) {
			material.SetColor ("_Color", Color.blue);
		}
	}
	public static void setDataPointColor(float alpha) {
		if (material != null) {
			Color newcolor = material.GetColor ("_Color");
			newcolor.a = alpha;
			material.SetColor ("_Color", newcolor);
		}
	}
}