using UnityEngine;
using UnityEditor;
using UnityEngine.UI;

public class Timer : MonoBehaviour {

	public Image threeStars;
	public Image twoStars;
	public Image oneStar;
	public bool runTime;
	public float threeStarsTime = 30.0f;
	public float twoStarsTime = 30.0f;
	public float oneStarTime = 30.0f;

	// Update is called once per frame
	void Update () 
	{
		if (threeStars.fillAmount > 0 && runTime == true) {
			threeStars.fillAmount -= 1f / threeStarsTime * Time.deltaTime;
		} else {
			
			if (twoStars.fillAmount > 0) {
				twoStars.fillAmount -= 1f / twoStarsTime * Time.deltaTime;
			} else {
				
				if (oneStar.fillAmount > 0) {
					oneStar.fillAmount -= 1f / oneStarTime * Time.deltaTime;
				}
			}
		}
	}
}