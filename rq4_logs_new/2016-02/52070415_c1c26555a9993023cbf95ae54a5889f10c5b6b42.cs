using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class ChangeSceneEffect : MonoBehaviour {

	public int sceneNumber;
	public Color color;
	public float alpha;
	public float duration;
	public bool ignoreTimeScale;
	public bool useAlpha;
/*
	void Start()
	{	
		AlphaFadetoColor();
		//StartCoroutine (FadeAndLoadScene ());	
	}
*/
	public void FadeandLoadScene()
	{	
		StartCoroutine (FadeAndLoadScene ());
		AlphaFadetoColor();
	}

	public void AlphaFadetoColor()
	{	
		GetComponent<Image>().CrossFadeAlpha(alpha,duration,ignoreTimeScale);
	}

	IEnumerator FadeAndLoadScene()
	{
		GetComponent<Image>().CrossFadeColor(color,duration,ignoreTimeScale,useAlpha);
		yield return new WaitForSeconds (duration);
		Application.LoadLevel(sceneNumber);
	}
}