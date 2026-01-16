using UnityEngine;
using UnityEngine.UI;

public class hiscore : MonoBehaviour
{
	static int highscore;
	public static int score;
	public Text text;

	// Set highscoreText up in the inspector.
	public Text highscoreText;
	public speed_variation speed;

	void Awake()
	{
		
		score = 0;
		highscore = PlayerPrefs.GetInt("highscore");
		highscoreText.text = "HishScore:" + highscore;
	}

	void Update()
	{	score =  (int)speed.point  ;
		if (score > highscore)
		{
			highscore = score;
			PlayerPrefs.SetInt("highscore", highscore);
			highscoreText.text = "Highscore: " + highscore;
		}
		text.text = "Score: " + score;
	}
}