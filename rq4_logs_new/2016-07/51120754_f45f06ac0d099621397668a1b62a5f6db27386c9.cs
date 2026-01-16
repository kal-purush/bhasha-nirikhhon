using UnityEngine;
using System.Collections;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class LoadingBarFadeInOut : MonoBehaviour {
	//the speed at which the fading happens
	public float alphaChangeSpeed;
	//the actual image of the bar
	private Image barImage;
	//bool to start fading
	private bool startFading;
	//bool to determine whether the image is fading in out out
	private bool increaseAlpha;
	//temp color variable
	private Color color;

	// Use this for initialization
	void Start () {
		//get the image
		barImage = gameObject.GetComponent<Image> ();
		startFading = false;
		increaseAlpha = false;
		color = new Color (1, 1, 1, 1);
	}
	
	// Update is called once per frame
	void Update () {
		//if the fading is starting
		if (startFading) 
		{
			//if the alpha is to increase
			if (increaseAlpha) 
			{
				//increment the alpha
				color.a += Time.deltaTime * alphaChangeSpeed;
				//if alpha is at max
				if (color.a >= 1f) 
				{	//then switch boolean so the alpha can decrease
					increaseAlpha = !increaseAlpha;
				}
			} 
			else if (!increaseAlpha) 
			{
				color.a -= Time.deltaTime * alphaChangeSpeed;
				if (color.a <= 0) 
				{
					increaseAlpha = !increaseAlpha;
				}
			}
			//set the new alpha to the image
			barImage.color = color;
		}
	
	}
	//function to activate fading
	public void beginFading()
	{
		startFading = true;
	}
}