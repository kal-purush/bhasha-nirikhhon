using UnityEngine;
using System.Collections;


// Class gathering utility methods.
public static class Utils {


	// Return true if the 2 vectors are almost equal
	public static bool CompareVector(Vector3 v1, Vector3 v2, float eps) {
		return CompareFloat(v1.x, v2.x, eps) && CompareFloat(v1.y, v2.y, eps) && CompareFloat(v1.z, v2.z, eps) ;
	}

	// Return true if the 2 floats are almost equal
	public static bool CompareFloat(float f1, float f2, float eps) {
		return ( f1 + eps > f2 ) && ( f1 - eps < f2 ); 
	}
}