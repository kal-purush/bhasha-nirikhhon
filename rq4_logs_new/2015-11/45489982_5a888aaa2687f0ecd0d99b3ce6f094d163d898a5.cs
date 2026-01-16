using UnityEngine;
using System.Collections;

public class StartGame : MonoBehaviour {

	public Sprite start;
	public Sprite startActive;

	private GameObject menu;
	private SpriteRenderer spriteRenderer;

	void Start()
	{
		spriteRenderer = GetComponent<SpriteRenderer> ();
		menu = GameObject.FindGameObjectWithTag ("Menu");
	}

	void OnMouseEnter()
	{
		spriteRenderer.sprite = startActive;
	}

	void OnMouseExit()
	{
		spriteRenderer.sprite = start;
	}

	void OnMouseDown()
	{
		Application.LoadLevel ("Playground1");
	}
}