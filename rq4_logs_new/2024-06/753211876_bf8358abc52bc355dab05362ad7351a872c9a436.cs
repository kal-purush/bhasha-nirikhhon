using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class AbrirUrl : MonoBehaviour
{
	[SerializeField] private AudioClip sound = null;
	public Color color = Color.red;
	public Color clickedColor = Color.red;
    public string url = "https://github.com/dannycata/ColourGame";
	private AudioSource audioSource = null;
	private Text buttonText;

    void Start()
    {
        buttonText = GetComponentInChildren<Text>();
		buttonText.color = color;
		Camera mainCamera = Camera.main;
		audioSource = mainCamera.GetComponent<AudioSource>();
        Button btn = GetComponent<Button>();
        btn.onClick.AddListener(OpenURL);
    }

    public void OpenURL()
    {
		buttonText.color = clickedColor;
		audioSource.PlayOneShot(sound);
        Application.OpenURL(url);
    }
}