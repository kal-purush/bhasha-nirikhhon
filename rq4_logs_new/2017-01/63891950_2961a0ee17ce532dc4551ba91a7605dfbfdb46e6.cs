using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Fade : MonoBehaviour {

	public  IEnumerator Fade3D (Transform t, float targetAlpha, bool isVanish, float duration)
     {
         Renderer sr = t.GetComponent<Renderer> ();
         float diffAlpha = (targetAlpha - sr.material.color.a);
 
         float counter = 0;
         while (counter < duration) {
             float alphaAmount = sr.material.color.a + (Time.deltaTime * diffAlpha) / duration;
             sr.material.color = new Color (sr.material.color.r, sr.material.color.g, sr.material.color.b, alphaAmount);
             counter += Time.deltaTime;

             yield return null;
         }
         sr.material.color = new Color (sr.material.color.r, sr.material.color.g, sr.material.color.b, targetAlpha);
         if (isVanish) {
	         sr.transform.gameObject.SetActive (false);
         }
     }
}