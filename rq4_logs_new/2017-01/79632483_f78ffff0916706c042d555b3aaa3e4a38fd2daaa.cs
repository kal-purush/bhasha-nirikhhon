using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UniRx;

public class ScoreManager : MonoBehaviour {

	public static ScoreManager instance;
	private Text scoreText;

	public ReactiveProperty<float> score { get; private set; }

	void Awake () {
		instance = this;
		score = new ReactiveProperty<float> ();
	}

	// Use this for initialization
	void Start () {
		scoreText = GetComponent<Text> ();
		score.Subscribe (score => {
			scoreText.text = "SCORE: " + score;
		});
	}

	public void incrementScore(float f) {
		score.Value += f;
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}