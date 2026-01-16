using UnityEngine;
using System.Collections;

public class SelectionMenu : MonoBehaviour 
{
	public GameObject selection;
	public GameObject selectionColor;
	public GameObject playButton;
	public GameObject optionsButton;
	public GameObject creditsButton;
	public GameObject exitButton;
	public GameObject banner;
	public GameObject pressStart;
	public GameObject blackPlane;

	private bool pressed = false;
	private float framesCounter = 1;

	private bool menuActive = false;

	private float alphaStart = 1.0f;
	private bool alphaStartDown = true;
	private bool changeToOptions = false;

	private float alphaBanner = 0.0f;
	private bool showOptions = false;
	private float alphaOptions = 0.0f;

	private bool menuReady = false;

	private bool loadScene = false;
	private float blackPlaneAlpha = 0.0f;

	void Start()
	{
		selection.transform.position = playButton.transform.position;

		selection.GetComponent<MeshRenderer> ().enabled = false;
		foreach(Transform child in selectionColor.transform) child.transform.GetComponent<MeshRenderer>().enabled = false;
		selectionColor.GetComponent<MeshRenderer> ().enabled = false;

		playButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		optionsButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		creditsButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		exitButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		banner.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaBanner);
		pressStart.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaStart);
	

	}

	void Update() 
	{
		if (!menuActive) 
		{
			if(!changeToOptions)
			{
				if (alphaStartDown)
				{
					alphaStart -= 0.01f;
					if(alphaStart <= 0) alphaStartDown = false;
				}
				else
				{
					alphaStart += 0.01f;
					if(alphaStart >= 1.0f) alphaStartDown = true;
				}


				if (Input.anyKey) changeToOptions = true;
			}

			else
			{
				if (alphaStart > 0.0f) 	alphaStart -= 0.1f;

				else 
				{
					alphaStart = 0.0f;
					menuActive = true;
				}
			}
		}

		else
		{
			if (alphaBanner < 1.0f) alphaBanner += 0.005f;
			else
			{ 
				alphaBanner = 1.0f;
				showOptions = true;
			}

			if (showOptions)
			{
				if (alphaOptions < 1.0f) alphaOptions += 0.005f;
				else alphaOptions = 1.0f;

				if (alphaOptions > 0.2f) menuReady = true;
			}

			if (menuReady)
			{
				selection.GetComponent<MeshRenderer> ().enabled = true;
				selectionColor.GetComponent<MeshRenderer> ().enabled = true;
				foreach(Transform child in selectionColor.transform) child.transform.GetComponent<MeshRenderer>().enabled = true;

				if (pressed)
					framesCounter -= Time.deltaTime * 10;
				if (framesCounter <= 0) {
					pressed = false;
					framesCounter = 1;
				}

				if (selection.transform.position == playButton.transform.position) {
					//Debug.Log("Play");
					if (Input.GetKeyDown (KeyCode.S) && !pressed) {
						selection.transform.position = optionsButton.transform.position;
						pressed = true;
					} else if (Input.GetKeyDown (KeyCode.W) && !pressed) {
						selection.transform.position = exitButton.transform.position;
						pressed = true;
					}
					if (Input.GetKeyDown (KeyCode.Return) || Input.GetKeyDown (KeyCode.Space))
					{
						loadScene = true;
					}
						
				}

				if (selection.transform.position == optionsButton.transform.position) {
					//Debug.Log("Options");
					if (Input.GetKeyDown (KeyCode.W) && !pressed) {
						selection.transform.position = playButton.transform.position;
						pressed = true;
					} else if (Input.GetKeyDown (KeyCode.S) && !pressed) {
						selection.transform.position = creditsButton.transform.position;
						pressed = true;
					}
					if (Input.GetKeyDown (KeyCode.Return) || Input.GetKeyDown (KeyCode.Space))
						Application.LoadLevel ("Credits");
				}

				if (selection.transform.position == creditsButton.transform.position) {
					//Debug.Log("Credits");
					if (Input.GetKeyDown (KeyCode.W) && !pressed) {
						selection.transform.position = optionsButton.transform.position;
						pressed = true;
					} else if (Input.GetKeyDown (KeyCode.S) && !pressed) {
						selection.transform.position = exitButton.transform.position;
						pressed = true;
					}

					if (Input.GetKeyDown (KeyCode.Return) || Input.GetKeyDown (KeyCode.Space))
						Application.LoadLevel ("Credits");
				}

				if (selection.transform.position == exitButton.transform.position) {
					//Debug.Log("Exit");
					if (Input.GetKeyDown (KeyCode.W) && !pressed) {
						selection.transform.position = creditsButton.transform.position;
						pressed = true;
						Debug.Log ("sale");
					} else if (Input.GetKeyDown (KeyCode.S) && !pressed) {
						selection.transform.position = playButton.transform.position;
						pressed = true;
					}

					if (Input.GetKeyDown (KeyCode.Return) || Input.GetKeyDown (KeyCode.Space))
						Application.Quit ();
				}

				if (loadScene)
				{
					if (blackPlane.GetComponent<MeshRenderer>().enabled == false) blackPlane.GetComponent<MeshRenderer>().enabled = true;
					if (blackPlaneAlpha < 1.0f) blackPlaneAlpha += 0.05f;
					else 
					{
						blackPlaneAlpha = 1.0f;
						Application.LoadLevel ("Loading");
					}
				}
			}

			selectionColor.transform.position = new Vector3 (selectionColor.transform.position.x, selection.transform.position.y, selectionColor.transform.position.z);	
		}

		pressStart.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaStart);
		playButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		optionsButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		creditsButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		exitButton.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaOptions);
		banner.GetComponent<SpriteRenderer> ().color = new Color (1f, 1f, 1f, alphaBanner);

		blackPlane.GetComponent<Renderer> ().material.color = new Color (0, 0, 0, blackPlaneAlpha);
	}
}