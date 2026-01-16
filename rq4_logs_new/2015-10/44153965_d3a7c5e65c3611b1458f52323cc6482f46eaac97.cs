using UnityEngine;
using UnityEngine.UI;
using System.Collections;

public class JoyDisplay : MonoBehaviour {

	public Text joyText;
	public Image joyEmoji;
	public Sprite[] emoji;
	public Color bad;
	public Color great;

	private float joyStats;
	private float colorChange;

	// Use this for initialization
	void Start () {
		joyStats = BanyaRating.satisfaction;
		joyText.text = (int)joyStats + "%";
	
	}
	
	// Update is called once per frame
	void Update () {
		BanyaRating.HappyKimans();
		JoyEmojiColor();
	}

	void JoyEmojiColor () {
		joyStats = BanyaRating.satisfaction;
		//colorChange = joyStats * 0.01f;
		joyText.text = (int)joyStats + "%";

	}
}