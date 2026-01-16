using UnityEngine;
using System.Collections;

public class ScoreManager {
	static string HIGHT_SCORE = "highscore";

	public int getHighScore() {
		return PlayerPrefs.GetInt (HIGHT_SCORE, 0);
	}

	public bool saveScore(int score) {
		int highscore = getHighScore ();

		if (score > highscore) {
			PlayerPrefs.SetInt(HIGHT_SCORE, score);
			return true;
		}
		return false;
	}
}